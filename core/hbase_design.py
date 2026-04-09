# core/hbase_design.py
class HBaseRowKeyDesign:
    """HBase Row Key design patterns"""
    
    @staticmethod
    def create_salted_key(store_id, timestamp, txn_id, num_salts=10):
        """Create salted row key for uniform distribution"""
        import hashlib
        
        # Create salt based on store_id hash
        hash_val = int(hashlib.md5(store_id.encode()).hexdigest()[:8], 16)
        salt = hash_val % num_salts
        
        # Reverse timestamp for recent-first queries
        max_ts = 9999999999999   # Max 64-bit timestamp
        reverse_ts = max_ts - int(timestamp.timestamp() * 1000)
        
        # Last 8 chars of transaction ID for uniqueness
        txn_suffix = txn_id[-8:]
        return f"{salt:02d}_{store_id}_{reverse_ts}_{txn_suffix}"

    @staticmethod
    def create_store_date_key(store_id, date, txn_id):
        """Optimized for queries by store and date"""
        date_str = date.strftime("%Y%m%d")
        txn_hash = txn_id[-4:]
        return f"{store_id}_{date_str}_{txn_hash}"