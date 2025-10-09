#!/usr/bin/env python3
"""
Large Python test file for token splitting logic testing.
This file contains approximately 22,000 tokens with diverse Python constructs.
"""

import sys
import os
import json
import re
import math
import random
import datetime
import collections
import itertools
import functools
import hashlib
import base64
import threading
import asyncio
import typing
from typing import List, Dict, Tuple, Optional, Union, Any, Set, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum, auto
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path

# Global constants for the application
MAX_BUFFER_SIZE = 8192
DEFAULT_TIMEOUT = 30
API_VERSION = "2.1.0"
SUPPORTED_ENCODINGS = ['utf-8', 'ascii', 'latin-1', 'utf-16']
DEBUG_MODE = False
CACHE_ENABLED = True
MAX_RETRY_ATTEMPTS = 3
CONNECTION_POOL_SIZE = 10
LOG_LEVEL = "INFO"
DATABASE_URL = "postgresql://localhost/testdb"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Type variables for generic programming
T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')

class ConfigurationError(Exception):
    """Custom exception for configuration related errors."""
    def __init__(self, message: str, error_code: int = None):
        super().__init__(message)
        self.error_code = error_code
        self.timestamp = datetime.datetime.now()
    
    def __str__(self):
        return f"ConfigurationError({self.error_code}): {super().__str__()} at {self.timestamp}"

class ValidationError(Exception):
    """Custom exception for validation errors in data processing."""
    def __init__(self, field_name: str, value: Any, expected_type: type):
        self.field_name = field_name
        self.value = value
        self.expected_type = expected_type
        super().__init__(f"Validation failed for field '{field_name}': expected {expected_type.__name__}, got {type(value).__name__}")

class NetworkError(Exception):
    """Custom exception for network-related errors."""
    def __init__(self, message: str, retry_count: int = 0):
        super().__init__(message)
        self.retry_count = retry_count
        self.can_retry = retry_count < MAX_RETRY_ATTEMPTS

class Status(Enum):
    """Enumeration for various status states in the application."""
    PENDING = auto()
    PROCESSING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    PAUSED = auto()
    RESUMED = auto()
    ARCHIVED = auto()
    DELETED = auto()

class Priority(Enum):
    """Priority levels for task scheduling."""
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4
    TRIVIAL = 5

@dataclass
class UserProfile:
    """Data class representing a user profile with various attributes."""
    user_id: int
    username: str
    email: str
    full_name: str
    created_at: datetime.datetime
    last_login: Optional[datetime.datetime] = None
    is_active: bool = True
    is_verified: bool = False
    role: str = "user"
    permissions: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    preferences: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Post-initialization validation and setup."""
        if not re.match(r'^[a-zA-Z0-9_]{3,20}$', self.username):
            raise ValidationError("username", self.username, str)
        if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', self.email):
            raise ValidationError("email", self.email, str)
        if self.user_id < 0:
            raise ValueError("User ID must be non-negative")
    
    def has_permission(self, permission: str) -> bool:
        """Check if user has a specific permission."""
        return permission in self.permissions or self.role == "admin"
    
    def update_last_login(self):
        """Update the last login timestamp."""
        self.last_login = datetime.datetime.now()
    
    def add_permission(self, permission: str):
        """Add a new permission to the user."""
        if permission not in self.permissions:
            self.permissions.append(permission)
    
    def remove_permission(self, permission: str):
        """Remove a permission from the user."""
        if permission in self.permissions:
            self.permissions.remove(permission)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the user profile to a dictionary."""
        return {
            'user_id': self.user_id,
            'username': self.username,
            'email': self.email,
            'full_name': self.full_name,
            'created_at': self.created_at.isoformat(),
            'last_login': self.last_login.isoformat() if self.last_login else None,
            'is_active': self.is_active,
            'is_verified': self.is_verified,
            'role': self.role,
            'permissions': self.permissions.copy(),
            'metadata': self.metadata.copy(),
            'preferences': self.preferences.copy()
        }

@dataclass
class Task:
    """Represents a task in the task management system."""
    task_id: str
    name: str
    description: str
    status: Status
    priority: Priority
    assigned_to: Optional[int] = None
    created_by: int = 0
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    due_date: Optional[datetime.datetime] = None
    completed_at: Optional[datetime.datetime] = None
    tags: List[str] = field(default_factory=list)
    attachments: List[str] = field(default_factory=list)
    comments: List[Dict[str, Any]] = field(default_factory=list)
    
    def is_overdue(self) -> bool:
        """Check if the task is overdue."""
        if self.due_date and self.status not in [Status.COMPLETED, Status.CANCELLED]:
            return datetime.datetime.now() > self.due_date
        return False
    
    def add_comment(self, user_id: int, text: str):
        """Add a comment to the task."""
        comment = {
            'user_id': user_id,
            'text': text,
            'timestamp': datetime.datetime.now().isoformat()
        }
        self.comments.append(comment)
        self.updated_at = datetime.datetime.now()
    
    def change_status(self, new_status: Status):
        """Change the status of the task."""
        old_status = self.status
        self.status = new_status
        self.updated_at = datetime.datetime.now()
        if new_status == Status.COMPLETED:
            self.completed_at = datetime.datetime.now()
        return old_status, new_status

class DataProcessor(ABC):
    """Abstract base class for data processors."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = self._setup_logger()
        self.metrics = collections.defaultdict(int)
        self.errors = []
        self.processing_start = None
        self.processing_end = None
    
    @abstractmethod
    def process(self, data: Any) -> Any:
        """Process the input data. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Validate the input data. Must be implemented by subclasses."""
        pass
    
    def _setup_logger(self):
        """Set up logging for the processor."""
        import logging
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        return logger
    
    def preprocess(self, data: Any) -> Any:
        """Preprocess data before main processing."""
        self.processing_start = datetime.datetime.now()
        self.logger.info(f"Starting preprocessing at {self.processing_start}")
        return data
    
    def postprocess(self, data: Any) -> Any:
        """Postprocess data after main processing."""
        self.processing_end = datetime.datetime.now()
        duration = (self.processing_end - self.processing_start).total_seconds()
        self.logger.info(f"Processing completed in {duration:.2f} seconds")
        self.metrics['processing_time'] = duration
        return data
    
    def get_metrics(self) -> Dict[str, Any]:
        """Return processing metrics."""
        return dict(self.metrics)
    
    def reset_metrics(self):
        """Reset all metrics to initial state."""
        self.metrics.clear()
        self.errors.clear()
        self.processing_start = None
        self.processing_end = None

class TextDataProcessor(DataProcessor):
    """Concrete implementation for processing text data."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.encoding = config.get('encoding', 'utf-8')
        self.max_length = config.get('max_length', 10000)
        self.strip_whitespace = config.get('strip_whitespace', True)
        self.lowercase = config.get('lowercase', False)
        self.remove_punctuation = config.get('remove_punctuation', False)
        self.stopwords = set(config.get('stopwords', []))
    
    def process(self, data: str) -> str:
        """Process text data according to configuration."""
        if not self.validate(data):
            raise ValidationError("data", data, str)
        
        data = self.preprocess(data)
        
        # Strip whitespace if configured
        if self.strip_whitespace:
            data = data.strip()
            data = re.sub(r'\s+', ' ', data)
        
        # Convert to lowercase if configured
        if self.lowercase:
            data = data.lower()
        
        # Remove punctuation if configured
        if self.remove_punctuation:
            import string
            data = data.translate(str.maketrans('', '', string.punctuation))
        
        # Remove stopwords if configured
        if self.stopwords:
            words = data.split()
            words = [w for w in words if w not in self.stopwords]
            data = ' '.join(words)
        
        # Truncate if exceeds max length
        if len(data) > self.max_length:
            data = data[:self.max_length]
            self.logger.warning(f"Text truncated to {self.max_length} characters")
        
        self.metrics['characters_processed'] += len(data)
        self.metrics['words_processed'] += len(data.split())
        
        return self.postprocess(data)
    
    def validate(self, data: Any) -> bool:
        """Validate that data is a string."""
        return isinstance(data, str)
    
    def tokenize(self, text: str) -> List[str]:
        """Tokenize text into words."""
        return text.split()
    
    def calculate_statistics(self, text: str) -> Dict[str, Any]:
        """Calculate various statistics about the text."""
        words = self.tokenize(text)
        unique_words = set(words)
        
        stats = {
            'total_characters': len(text),
            'total_words': len(words),
            'unique_words': len(unique_words),
            'average_word_length': sum(len(w) for w in words) / len(words) if words else 0,
            'longest_word': max(words, key=len) if words else '',
            'shortest_word': min(words, key=len) if words else '',
            'word_frequency': collections.Counter(words)
        }
        
        return stats

class NumericDataProcessor(DataProcessor):
    """Processor for numeric data with statistical operations."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.precision = config.get('precision', 2)
        self.normalize = config.get('normalize', False)
        self.scale_factor = config.get('scale_factor', 1.0)
        self.outlier_threshold = config.get('outlier_threshold', 3.0)
    
    def process(self, data: List[float]) -> List[float]:
        """Process numeric data."""
        if not self.validate(data):
            raise ValidationError("data", data, list)
        
        data = self.preprocess(data)
        
        # Apply scaling
        data = [x * self.scale_factor for x in data]
        
        # Normalize if configured
        if self.normalize:
            data = self.normalize_data(data)
        
        # Remove outliers
        data = self.remove_outliers(data)
        
        # Round to specified precision
        data = [round(x, self.precision) for x in data]
        
        self.metrics['numbers_processed'] += len(data)
        
        return self.postprocess(data)
    
    def validate(self, data: Any) -> bool:
        """Validate that data is a list of numbers."""
        if not isinstance(data, list):
            return False
        return all(isinstance(x, (int, float)) for x in data)
    
    def normalize_data(self, data: List[float]) -> List[float]:
        """Normalize data to 0-1 range."""
        if not data:
            return data
        min_val = min(data)
        max_val = max(data)
        if max_val == min_val:
            return [0.5] * len(data)
        return [(x - min_val) / (max_val - min_val) for x in data]
    
    def remove_outliers(self, data: List[float]) -> List[float]:
        """Remove outliers based on z-score."""
        if len(data) < 3:
            return data
        
        mean = sum(data) / len(data)
        std_dev = math.sqrt(sum((x - mean) ** 2 for x in data) / len(data))
        
        if std_dev == 0:
            return data
        
        filtered = []
        removed = 0
        for x in data:
            z_score = abs((x - mean) / std_dev)
            if z_score <= self.outlier_threshold:
                filtered.append(x)
            else:
                removed += 1
        
        if removed > 0:
            self.logger.info(f"Removed {removed} outliers")
            self.metrics['outliers_removed'] = removed
        
        return filtered
    
    def calculate_statistics(self, data: List[float]) -> Dict[str, float]:
        """Calculate statistical measures for the data."""
        if not data:
            return {}
        
        sorted_data = sorted(data)
        n = len(data)
        
        stats = {
            'count': n,
            'sum': sum(data),
            'mean': sum(data) / n,
            'min': min(data),
            'max': max(data),
            'range': max(data) - min(data),
            'median': sorted_data[n // 2] if n % 2 else (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2,
            'variance': sum((x - sum(data) / n) ** 2 for x in data) / n,
        }
        stats['std_dev'] = math.sqrt(stats['variance'])
        
        return stats

class DatabaseConnection:
    """Simulated database connection class."""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.is_connected = False
        self.transaction_active = False
        self.cursor = None
        self.queries_executed = 0
        self.last_query = None
        self.connection_pool = []
    
    def connect(self) -> bool:
        """Establish database connection."""
        if self.is_connected:
            return True
        
        # Simulate connection logic
        import time
        time.sleep(0.01)  # Simulate connection delay
        self.is_connected = True
        self.cursor = self._create_cursor()
        return True
    
    def disconnect(self):
        """Close database connection."""
        if self.transaction_active:
            self.rollback()
        self.is_connected = False
        self.cursor = None
    
    def _create_cursor(self):
        """Create a database cursor."""
        class Cursor:
            def __init__(self):
                self.row_count = 0
                self.last_query = None
            
            def execute(self, query: str, params: Optional[Tuple] = None):
                self.last_query = query
                self.row_count = random.randint(0, 100)
                return self
            
            def fetchone(self):
                return {"id": 1, "data": "sample"}
            
            def fetchall(self):
                return [{"id": i, "data": f"row_{i}"} for i in range(5)]
            
            def fetchmany(self, size: int = 10):
                return [{"id": i, "data": f"row_{i}"} for i in range(min(size, 5))]
        
        return Cursor()
    
    def execute(self, query: str, params: Optional[Tuple] = None) -> Any:
        """Execute a database query."""
        if not self.is_connected:
            raise ConnectionError("Not connected to database")
        
        self.last_query = query
        self.queries_executed += 1
        
        # Simulate query execution
        result = self.cursor.execute(query, params)
        return result
    
    def begin_transaction(self):
        """Start a new database transaction."""
        if self.transaction_active:
            raise RuntimeError("Transaction already active")
        self.transaction_active = True
    
    def commit(self):
        """Commit the current transaction."""
        if not self.transaction_active:
            raise RuntimeError("No active transaction")
        self.transaction_active = False
    
    def rollback(self):
        """Rollback the current transaction."""
        if not self.transaction_active:
            raise RuntimeError("No active transaction")
        self.transaction_active = False
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        self.begin_transaction()
        try:
            yield self
            self.commit()
        except Exception as e:
            self.rollback()
            raise e
    
    def create_table(self, table_name: str, columns: Dict[str, str]) -> bool:
        """Create a new table in the database."""
        column_definitions = []
        for col_name, col_type in columns.items():
            column_definitions.append(f"{col_name} {col_type}")
        
        query = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"
        self.execute(query)
        return True
    
    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """Insert data into a table."""
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ['?' for _ in values]
        
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        result = self.execute(query, tuple(values))
        return result.row_count
    
    def select(self, table: str, columns: List[str] = None, where: str = None) -> List[Dict]:
        """Select data from a table."""
        if columns:
            column_str = ', '.join(columns)
        else:
            column_str = '*'
        
        query = f"SELECT {column_str} FROM {table}"
        if where:
            query += f" WHERE {where}"
        
        result = self.execute(query)
        return result.fetchall()
    
    def update(self, table: str, data: Dict[str, Any], where: str) -> int:
        """Update data in a table."""
        set_clauses = [f"{k} = ?" for k in data.keys()]
        query = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {where}"
        result = self.execute(query, tuple(data.values()))
        return result.row_count
    
    def delete(self, table: str, where: str) -> int:
        """Delete data from a table."""
        query = f"DELETE FROM {table} WHERE {where}"
        result = self.execute(query)
        return result.row_count

class CacheManager(Generic[K, V]):
    """Generic cache manager with LRU eviction policy."""
    
    def __init__(self, max_size: int = 100, ttl: int = 3600):
        self.max_size = max_size
        self.ttl = ttl
        self.cache: Dict[K, Tuple[V, datetime.datetime]] = {}
        self.access_count: Dict[K, int] = collections.defaultdict(int)
        self.access_time: Dict[K, datetime.datetime] = {}
        self.hits = 0
        self.misses = 0
        self.evictions = 0
