import unittest
import pandas as pd
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

from application.use_cases.inventory.inventory_matcher import InventoryMatcher
from application.use_cases.inventory.inventory_processor import InventoryProcessor
from domain.entities.inventory_entity import InventoryMatch
from application.use_cases.audit_processor import AuditProcessor

class TestInventoryMatcher(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method"""
        # Create sample inventory data
        self.inventory_data = {
            'Material Designator': ['PART001', 'PART001', 'PART002'],
            'Organization Code': ['01', '02', '01'],
            'Quantity': [100, 200, 150],
            'AGING 0-30 QUANTITY': [50, 100, 75],
            'AGING 0-30 VALUE': [5000, 10000, 7500],
            'AGING 31-60 QUANTITY': [50, 100, 75],
            'AGING 31-60 VALUE': [5000, 10000, 7500]
        }
        self.inventory_df = pd.DataFrame(self.inventory_data)
        self.matcher = InventoryMatcher()

    def test_load_inventory(self):
        """Test loading inventory data"""
        self.matcher.load_inventory(self.inventory_df)
        self.assertIsNotNone(self.matcher._inventory_data)
        self.assertEqual(len(self.matcher._inventory_data), 3)

    def test_check_inventory_exact_match(self):
        """Test checking inventory with exact part number match"""
        self.matcher.load_inventory(self.inventory_df)
        result = self.matcher.check_inventory('PART001', '01')
        self.assertTrue(result.has_inventory)
        self.assertEqual(result.quantity, 100)

    def test_check_inventory_no_match(self):
        """Test checking inventory with non-existent part"""
        self.matcher.load_inventory(self.inventory_df)
        result = self.matcher.check_inventory('NONEXISTENT', '01')
        self.assertFalse(result.has_inventory)
        self.assertEqual(result.quantity, 0)

class TestInventoryProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        self.processor = InventoryProcessor()
        self.inventory_data = {
            'Material Designator': ['PART001', 'PART002'],
            'Organization Code': ['01', '02'],
            'AGING 0-30 QUANTITY': [100, 200],
            'AGING 0-30 VALUE': [1000, 2000],
            'AGING 31-60 QUANTITY': [50, 100],
            'AGING 31-60 VALUE': [500, 1000]
        }
        self.inventory_df = pd.DataFrame(self.inventory_data)

    def test_extract_aging_info(self):
        """Test extraction of aging information"""
        aging_info = self.processor._extract_aging_info(self.inventory_df)
        self.assertIn('0-30', aging_info)
        self.assertIn('31-60', aging_info)
        self.assertEqual(aging_info['0-30']['quantity'], 300)
        self.assertEqual(aging_info['31-60']['quantity'], 150)

    def test_advanced_part_match(self):
        """Test advanced part number matching"""
        self.assertTrue(self.processor.advanced_part_match('PART001', 'PART001'))
        self.assertTrue(self.processor.advanced_part_match('PART001.A', 'PART001'))
        self.assertFalse(self.processor.advanced_part_match('PART001', 'PART002'))

class TestAuditProcessorInventory(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        self.audit_processor = AuditProcessor()
        
        # Sample inventory data
        self.inventory_data = {
            'Material Designator': ['PART001', 'PART001', 'PART002'],
            'Organization Code': ['01', '02', '01'],
            'Quantity': [100, 200, 150]
        }
        self.inventory_df = pd.DataFrame(self.inventory_data)
        
        # Sample audit data
        self.audit_data = {
            'Part Number': ['PART001', 'PART001', 'PART002'],
            'Organization': ['01', '02', '01'],
            'Serial Control': ['YES', 'NO', 'YES']
        }
        self.audit_df = pd.DataFrame(self.audit_data)

    def test_check_inventory_for_mismatches(self):
        """Test inventory checking for mismatched parts"""
        mismatched_parts = ['PART001']
        org_destination = ['01', '02']
        
        results = self.audit_processor._check_inventory_for_mismatches(
            mismatched_parts,
            self.audit_df,
            org_destination,
            self.inventory_df
        )
        
        # Verify results structure and content
        self.assertIn('PART001_01', results)
        self.assertIn('PART001_02', results)
        self.assertEqual(results['PART001_01']['quantity'], 100)
        self.assertEqual(results['PART001_02']['quantity'], 200)

    def test_check_inventory_without_inventory_data(self):
        """Test inventory checking when no inventory data is provided"""
        mismatched_parts = ['PART001']
        org_destination = ['01']
        
        results = self.audit_processor._check_inventory_for_mismatches(
            mismatched_parts,
            self.audit_df,
            org_destination,
            None
        )
        
        self.assertIn('PART001_01', results)
        self.assertEqual(results['PART001_01']['quantity'], 0)
        self.assertFalse(results['PART001_01']['has_inventory'])

if __name__ == '__main__':
    unittest.main()