import psycopg2
from psycopg2 import Error
from typing import List, Dict, Any, Optional, Tuple, Union
from contextlib import contextmanager
import logging
import csv
import os
import shutil
from datetime import datetime
import sys
import pandas as pd

class PostgresLogger:
    """Custom logger for database operations"""
    
    def __init__(self, log_file: str = "log/db_log.csv"):
        """Initialize logger with log file path"""
        self.log_file = log_file
        self.log_dir = os.path.dirname(log_file)
        self.max_log_size = 5 * 1024 * 1024  # 5MB
        self.max_backups = 3
        
        # Create log directory if it doesn't exist
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
            
        # Create or check log file with headers if it doesn't exist
        if not os.path.exists(log_file):
            with open(log_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'operation', 'table', 'status', 'affected_rows', 'error', 'execution_time'])
    
    def _rotate_logs(self):
        """Rotate log files if size exceeds limit"""
        if os.path.exists(self.log_file) and os.path.getsize(self.log_file) > self.max_log_size:
            # Rotate existing backups
            for i in range(self.max_backups - 1, 0, -1):
                old_backup = f"{self.log_file}.{i}"
                new_backup = f"{self.log_file}.{i + 1}"
                if os.path.exists(old_backup):
                    if i == self.max_backups - 1:
                        os.remove(old_backup)
                    else:
                        shutil.move(old_backup, new_backup)
            
            # Create new backup
            shutil.move(self.log_file, f"{self.log_file}.1")
            
            # Create new log file with headers
            with open(self.log_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'operation', 'table', 'status', 'affected_rows', 'error', 'execution_time'])
    
    def log(self, operation: str, table: str, status: str, affected_rows: int = 0, error: str = None, execution_time: float = 0):
        """Log database operation to CSV file"""
        self._rotate_logs()
        
        with open(self.log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().isoformat(),
                operation,
                table,
                status,
                affected_rows,
                error,
                f"{execution_time:.4f}"
            ])

class PostgresHandler:
    """
    A handler class for PostgreSQL database operations.
    Supports CRUD operations on tables and records with logging.
    """
    
    def __init__(self, dbname: str, user: str, password: str, host: str = 'localhost', port: str = '5432'):
        """Initialize database connection parameters and logger."""
        self.connection_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.logger = PostgresLogger()

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            yield conn
        except Error as e:
            raise Exception(f"Error connecting to PostgreSQL: {e}")
        finally:
            if conn is not None:
                conn.close()

    def create_table(self, table_name: str, columns: Dict[str, str]) -> bool:
        """Create a new table in the database."""
        start_time = datetime.now()
        columns_def = ', '.join([f"{col_name} {col_type}" for col_name, col_type in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    conn.commit()
                    execution_time = (datetime.now() - start_time).total_seconds()
                    self.logger.log('CREATE_TABLE', table_name, 'SUCCESS', execution_time=execution_time)
                    return True
        except Error as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.logger.log('CREATE_TABLE', table_name, 'ERROR', error=str(e), execution_time=execution_time)
            return False

    def insert_record(self, table_name: str, data: Dict[str, Any]) -> Optional[int]:
        """
        Insert a single record into a table and return its ID.
        
        Args:
            table_name: Name of the table
            data: Dictionary of column names and values
            
        Returns:
            Optional[int]: ID of the inserted record or None if insertion failed
        """
        start_time = datetime.now()
        columns = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values}) RETURNING id"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, list(data.values()))
                    record_id = cur.fetchone()[0]
                    conn.commit()
                    execution_time = (datetime.now() - start_time).total_seconds()
                    self.logger.log('INSERT', table_name, 'SUCCESS', affected_rows=1, execution_time=execution_time)
                    return record_id
        except Error as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.logger.log('INSERT', table_name, 'ERROR', error=str(e), execution_time=execution_time)
            return None

    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]]) -> List[int]:
        """
        Insert multiple records into a table and return their IDs.
        
        Returns:
            List[int]: List of inserted record IDs
        """
        if not data:
            return []
            
        start_time = datetime.now()
        columns = ', '.join(data[0].keys())
        values = ', '.join(['%s'] * len(data[0]))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values}) RETURNING id"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    record_ids = []
                    for record in data:
                        cur.execute(query, list(record.values()))
                        record_ids.append(cur.fetchone()[0])
                    conn.commit()
                    execution_time = (datetime.now() - start_time).total_seconds()
                    self.logger.log('BULK_INSERT', table_name, 'SUCCESS', 
                                  affected_rows=len(data), execution_time=execution_time)
                    return record_ids
        except Error as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.logger.log('BULK_INSERT', table_name, 'ERROR', error=str(e), execution_time=execution_time)
            return []

    def select_records(self, table_name: str, columns: List[str] = None, 
                      conditions: Dict[str, Any] = None, 
                      order_by: str = None, 
                      limit: int = None,
                      output_type: str = ("list", "pd.df")) -> List[Tuple]|pd.DataFrame:
        """Select records from a table with optional filtering and ordering."""
        start_time = datetime.now()
        cols = ', '.join(columns) if columns else '*'
        query = f"SELECT {cols} FROM {table_name}"
        
        params = []
        if conditions:
            where_clause = ' AND '.join([f"{k} = %s" for k in conditions.keys()])
            query += f" WHERE {where_clause}"
            params.extend(conditions.values())
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    # Get column names from cursor description
                    column_names = [desc[0] for desc in cur.description]
                    results = cur.fetchall()
                    execution_time = (datetime.now() - start_time).total_seconds()
                    self.logger.log('SELECT', table_name, 'SUCCESS', 
                                  affected_rows=len(results), execution_time=execution_time)
                    return results if output_type != "pd.df" else pd.DataFrame(results, columns=column_names)
        except Error as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.logger.log('SELECT', table_name, 'ERROR', error=str(e), execution_time=execution_time)
            return []

    def update_records(self, table_name: str, data: Dict[str, Any], 
                      conditions: Dict[str, Any]) -> int:
        """
        Update records in a table that match the conditions.
        
        Returns:
            int: Number of records updated
        """
        start_time = datetime.now()
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        where_clause = ' AND '.join([f"{k} = %s" for k in conditions.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        
        params = list(data.values()) + list(conditions.values())
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    affected_rows = cur.rowcount
                    conn.commit()
                    execution_time = (datetime.now() - start_time).total_seconds()
                    self.logger.log('UPDATE', table_name, 'SUCCESS', 
                                  affected_rows=affected_rows, execution_time=execution_time)
                    return affected_rows
        except Error as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.logger.log('UPDATE', table_name, 'ERROR', error=str(e), execution_time=execution_time)
            return 0

    def delete_records(self, table_name: str, conditions: Dict[str, Any]) -> int:
        """
        Delete records from a table that match the conditions.
        
        Returns:
            int: Number of records deleted
        """
        start_time = datetime.now()
        where_clause = ' AND '.join([f"{k} = %s" for k in conditions.keys()])
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, list(conditions.values()))
                    affected_rows = cur.rowcount
                    conn.commit()
                    execution_time = (datetime.now() - start_time).total_seconds()
                    self.logger.log('DELETE', table_name, 'SUCCESS', 
                                  affected_rows=affected_rows, execution_time=execution_time)
                    return affected_rows
        except Error as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.logger.log('DELETE', table_name, 'ERROR', error=str(e), execution_time=execution_time)
            return 0

    def execute_raw_query(self, query: str, params: tuple = None) -> Optional[List[Tuple]]:
        """Execute a raw SQL query with optional parameters."""
        start_time = datetime.now()
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    if query.strip().upper().startswith('SELECT'):
                        results = cur.fetchall()
                        execution_time = (datetime.now() - start_time).total_seconds()
                        self.logger.log('RAW_QUERY', 'N/A', 'SUCCESS', 
                                      affected_rows=len(results), execution_time=execution_time)
                        return results
                    else:
                        affected_rows = cur.rowcount
                        conn.commit()
                        execution_time = (datetime.now() - start_time).total_seconds()
                        self.logger.log('RAW_QUERY', 'N/A', 'SUCCESS', 
                                      affected_rows=affected_rows, execution_time=execution_time)
                        return None
        except Error as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.logger.log('RAW_QUERY', 'N/A', 'ERROR', error=str(e), execution_time=execution_time)
            return None