import unittest
import pandas as pd
import os
import shutil
import sys

from main import run_pipeline # adapt if necessary once pipeline is implemented
#Wrote tests independently, used gemini to help with set up of testing suite, wrote the individual tests myself unless otherwise marked
#checked code/debugged with cursor
class TestMarketPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls): # sets up temproary sandbox, runs once before all tests
        """Create a temporary sandbox for data files so we don't touch your real data."""
        cls.test_data_dir = "./tests/temp_data/"
        if not os.path.exists(cls.test_data_dir):
            os.makedirs(cls.test_data_dir)

    @classmethod
    def tearDownClass(cls): # tears down and removes sandbox after all tests are finished, runs once after all tests are finished
        """Clean up the sandbox after all tests are finished."""
        if os.path.exists(cls.test_data_dir):
            shutil.rmtree(cls.test_data_dir)

    def create_mock_environment(self, test_input_dict): 
        # looks at test requirement, if only give input for one specific file, it  will fill the rest with standard 'good' data to pass the 'Missing Files' check.
        """
        Helper to create all 5 CSV files required for a pipeline run.
        test_input_dict: { 'MarketName': {'observation_date': [...], 'closing_price': [...] } }
        """
        markets = ['DJCA', 'DJIA', 'DJTA', 'DJUA', 'SP500']
        for m in markets:
            file_path = os.path.join(self.test_data_dir, f"{m}.csv")
            # If a test case doesn't provide data for a specific market, 
            # we fill it with standard 'good' data to pass the 'Missing Files' check.
            data = test_input_dict.get(m, {
                'observation_date': ['2026-01-01', '2026-01-02'],
                'closing_price': [100.00, 101.00]
            })
            pd.DataFrame(data).to_csv(file_path, index=False, quoting=1)

    def tearDown(self): # occurs between each test so that the next test starts with a clean environment
        """Wipe files between tests so they don't interfere."""
        for f in os.listdir(self.test_data_dir):
            os.remove(os.path.join(self.test_data_dir, f))
    #NOTE: DID A FEW DIFFERENT STYLES OF TEST TO SEE WHICH ONES WORK/IF THEY ALL WORK 
    # --- CATEGORY 1: MARKET TIMING ---

    def test_weekend_gap(self):
        """TEST: Monday recognizes Friday as previous day (Expect +0.53%)"""
        input_data = {
            'DJIA': {
                'observation_date': ['2026-04-17', '2026-04-20'],
                'closing_price': [38000.00, 38201.40]
            }
        }
        self.create_mock_environment(input_data)
        
        # We expect run_pipeline to return a results object (like a dict of DataFrames)
        results = run_pipeline(data_path=self.test_data_dir)
        
        # Check the calculation for Monday (April 20)
        actual_change = results['DJIA'].iloc[-1]['daily_return']
        self.assertAlmostEqual(actual_change, 0.0053, places=4)
        # assertAlmostEqual is used to compare floats with a tolerance 
        #places=4 means 4 decimal places have to match

    def test_leap_year(self):
        """TEST: System accepts Feb 29 as a valid date in 2028."""
        input_data = {
            'SP500': {
                'observation_date': ['2028-02-28', '2028-02-29', '2028-03-01'],
                'closing_price': [5000, 5010, 5020]
            }
        }
        self.create_mock_environment(input_data)
        results = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(len(results['SP500']), 3)

    def test_holiday(self):
        """TEST: System recognizes holidays without crashing or returning errors."""
        input_data = {
            'DJCA': {
                'observation_date': ['2025-12-31', '2026-01-01', '2026-01-02'],
                'closing_price': [100.00, , 102.00]
            }
        }
        self.create_mock_environment(input_data)
        results = run_pipeline(data_path=self.test_data_dir)
        actual_change = results['DJCA'].iloc[-1]['daily_return']
        self.assertEqual(actual_change, .02) # 2% change from 100 to 102
    # --- CATEGORY 2: NUMERICAL ANOMALIES ---

    def test_negative_price(self):
        """TEST: Pipeline must REJECT negative prices."""
        input_data = {
            'DJTA': {'observation_date': ['2026-01-01'], 'closing_price': [-50.00]}
        }
        self.create_mock_environment(input_data)
        
        status = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Invalid Price")

    def test_zero_price(self):
        """TEST: Pipeline must REJECT zero prices."""
        input_data = {
            'DJUA': {'observation_date': ['2026-01-01'], 'closing_price': [0.00]}
        }
        self.create_mock_environment(input_data)
        status = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Invalid Price")

    def test_extreme_volatility(self):
        """TEST: Pipeline must warn on extreme volatility."""
        input_data = {
            'DJTA': {
                'observation_date': ['2026-01-03', '2026-01-04'], 
                'closing_price': [100.00, 10000.00]
            }
        }
        self.create_mock_environment(input_data)
        status = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "WARNING: Extreme Volatility")

    # --- CATEGORY 3: DATA INTEGRITY ---

    def test_sync_error(self):
        """TEST: Detects when one file has a date others do not."""
        input_data = {
            'DJIA': {'observation_date': ['2026-05-01'], 'closing_price': [100]},
            'SP500': {'observation_date': ['2026-05-02'], 'closing_price': [100]} # Sync Mismatch
        }
        self.create_mock_environment(input_data)
        status = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Files out of sync")
    def test_missing_files(self): 
        # may have to rewrite just given how the test suite is formatted to prevent this being an error
        """TEST: Rejects if any required file is missing."""
        input_data = {
            'DJIA': {'observation_date': ['2026-01-01'], 'closing_price': [100]},
            'SP500': {'observation_date': ['2026-01-02'], 'closing_price': [100]}
        }
        self.create_mock_environment(input_data)
        status = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Missing Files")

    def test_date_mismatch(self):
        """TEST: Rejects if dates are not in correct form : YYYY-MM-DD"""
        input_data = {
            'DJIA': {'observation_date': ['12-31-2026'], 'closing_price': [100]}
        }
        self.create_mock_environment(input_data)
        status = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Date in wrong format")

    def test_random_string(self):
        """TEST: Rejects if any column has a random string in it."""
        input_data = {
            'DJIA': {'observation_date': ['Tuesday'], 'closing_price': [100]}
        }
        self.create_mock_environment(input_data)
        status = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Random string in column")
        
    def test_dates_wrong_order(self):
        """TEST: rejects if dates are not in ascending order"""
        input_data = {
            'DJIA' : {
                'observation_date' : ['2026-03-12', '2026-03-11'],
                'closing_price':[100, 102]
            }
        }
        self.create_mock_environment(input_data)
        status = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Date in wrong order")

    def test_duplicate_dates(self):
        """TEST: Rejects if duplicate dates"""
        input_data = {
            'DJCA' : {
                'observation_date' : ['2026-03-12', '2026-03-12'],
                'closing_price':[100, 102]
            }
        }    

        self.create_mock_environment(input_data)
        status = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Duplicate Dates")

    #--ALL TESTS PAST THIS POINT WERE SUGGESTED BY GEMINI HAS MISSING FROM TESTING SUITE--
    def test_empty_file(self):
        """TEST: Rejects if file empy"""
        input_data = {
            'DJCA' : {
                'observation_date' : [],
                'closing_price':[]
            }
        }    

        self.create_mock_environment(input_data)
        status = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: empty file")
        

    def test_header_mismatch(self):
        """TEST: Rejects if column is 'Date' instead of 'observation_date'."""
        df = pd.DataFrame({'Date': ['2026-01-01'], 'closing_price': [100]})
        df.to_csv(os.path.join(self.test_data_dir, "DJIA.csv"), index=False)
        self.create_mock_environment({'DJIA': df})

        status = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Header mismatch")

    def test_trailing_whitespace_trim(self):
        """TEST: Trailing whitespace in date/price is trimmed and parsed."""
        df = pd.DataFrame({
            'observation_date': ['2026-04-17 ', '2026-04-18'],
            'closing_price': [' 38000.00', '38100.00 ']
        })
        df.to_csv(os.path.join(self.test_data_dir, "DJIA.csv"), index=False)
        self.create_mock_environment({})  # populate other required market files
        results = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(results['DJIA'].iloc[0]['observation_date'], '2026-04-17')
        self.assertEqual(results['DJIA'].iloc[0]['closing_price'], 38000.00)

    def test_non_numeric_price(self):
        """TEST: Non-numeric placeholders N/A and '.' are rejected"""
        df = pd.DataFrame({
            'observation_date': ['2026-04-17', '2026-04-18'],
            'closing_price': ['N/A', '.']
        })
        df.to_csv(os.path.join(self.test_data_dir, "SP500.csv"), index=False)
        self.create_mock_environment({})  # populate other required market files
        status = run_pipeline(data_path=self.test_data_dir)
        # Adjust to your pipeline contract if needed:
        self.assertIn(status, "REJECT: Missing Price")
    def test_extreme_precision(self):
        """TEST: High precision price is rounded to 2 decimals."""
        df = pd.DataFrame({
            'observation_date': ['2026-04-17'],
            'closing_price': [38000.1234567]
        })
        df.to_csv(os.path.join(self.test_data_dir, "DJIA.csv"), index=False)
        self.create_mock_environment({})  # populate other required market files
        results = run_pipeline(data_path=self.test_data_dir)
        self.assertAlmostEqual(results['DJIA'].iloc[0]['closing_price'], 38000.12, places=2)
    def test_range_mismatch(self):
        """TEST: Detects incomplete overlap when market date ranges start at different times."""
        input_data = {
            'DJIA': {
                'observation_date': ['2020-01-01', '2020-01-02'],
                'closing_price': [30000.0, 30100.0]
            },
            'SP500': {
                'observation_date': ['2022-01-01', '2022-01-0'],
                'closing_price': [4700.0, 4710.0]
            }
        }
        self.create_mock_environment(input_data)
        status = run_pipeline(data_path=self.test_data_dir)
        # If your implementation crops to overlap instead, change expectation accordingly.
        self.assertIn(status, ["WARNING: Incomplete overlap", "REJECT: Incomplete overlap"])

    def test_comma_formatting(self):
        """TEST: Price with comma '38,000.00' is parsed correctly as float."""
        # Note: We simulate the comma as a string in the CSV
        df = pd.DataFrame({'observation_date': ['2026-01-01'], 'closing_price': ['38,000.00']})
        df.to_csv(os.path.join(self.test_data_dir, "DJIA.csv"), index=False)
        # (Fill others with good data)
        self.create_mock_environment({'DJIA': df}) 

        results = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(results['DJIA'].iloc[0]['closing_price'], 38000.00)
if __name__ == '__main__':
    unittest.main()