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

    def create_mock_environment(self, test_input_dict, markets_to_create=None):
        """
        markets_to_create: List of tickers to actually build. 
        If None, builds all 5.
        """
        all_markets = ['DJCA', 'DJIA', 'DJTA', 'DJUA', 'SP500']
        target_markets = markets_to_create if markets_to_create is not None else all_markets
        
        for m in target_markets:
            file_path = os.path.join(self.test_data_dir, f"{m}.csv")
            data = test_input_dict.get(m, {
                'observation_date': ['2026-01-02', '2026-01-03'],
                'closing_price': [100.00, 101.00]
            })
            pd.DataFrame(data).to_csv(file_path, index=False)

    def tearDown(self): # occurs between each test so that the next test starts with a clean environment
        """Wipe files between tests so they don't interfere."""
        for f in os.listdir(self.test_data_dir):
            os.remove(os.path.join(self.test_data_dir, f))
    #NOTE: DID A FEW DIFFERENT STYLES OF TEST TO SEE WHICH ONES WORK/IF THEY ALL WORK 
    # --- CATEGORY 0: ESSENTIALS ---
    def test_custom_threshold_logic(self):
        """TEST: SP500 ignores 1.2% change while DJIA flags it."""
        input_data = {
            'DJIA': {'observation_date': ['2026-01-01', '2026-01-02'], 'closing_price': [100, 101.2]}, # 1.2% (Flag!)
            'SP500': {'observation_date': ['2026-01-01', '2026-01-02'], 'closing_price': [100, 101.2]} # 1.2% (Ignore!)
        }
        self.create_mock_environment(input_data)
        status, data = run_pipeline(data_path=self.test_data_dir)
        
        # Assert DJIA is in the breach report but SP500 is not
        self.assertEqual(status, "SUCCESS")
        tickers_flagged = [row['Ticker'] for row in data['breach_report']]
        self.assertIn('DJIA', tickers_flagged)
        self.assertNotIn('SP500', tickers_flagged)
    def test_week_over_week_flag(self):
        """TEST: Flags a 6% change over 5 trading days."""
        input_data = {
            'DJIA': {
                'observation_date': ['2026-01-01', '2026-01-02', '2026-01-03', '2026-01-04', '2026-01-05', '2026-01-08'],
                'closing_price': [100, 100, 100, 100, 100, 106] # 6% jump from Jan 1st
            }
        }
        self.create_mock_environment(input_data)
        status, data = run_pipeline(data_path=self.test_data_dir)
        
        self.assertEqual(status, "SUCCESS")
        self.assertTrue(any(r['Check_Type'] == 'WoW' for r in data['breach_report']))
    def test_consecutive_weekdays(self):
        """TEST: Pure math check for consecutive days (No gaps)."""
        input_data = {
            'SP500': {
                'observation_date': [
                    '2026-04-20', # Monday
                    '2026-04-21', # Tuesday
                    '2026-04-22'  # Wednesday
                ],
                'closing_price': [
                    100.00, 
                    101.50, # +1.5% DoD
                    102.515 # +1.0% DoD from Tuesday
                ]
            }
        }
        self.create_mock_environment(input_data)

        status, data = run_pipeline(data_path=self.test_data_dir)
        
        self.assertEqual(status, "SUCCESS")
        actual_change = data['processed_data']['SP500'].iloc[-1]['daily_return']
        self.assertAlmostEqual(actual_change, 0.01, places=4)
    def test_disabled_checks(self):
        """TEST: If WoW check is disabled, it shouldn't appear in results."""
        input_data = {
            'DJIA': {'observation_date': ['2026-01-01', '2026-01-08'], 'closing_price': [100, 110]}
        }
        self.create_mock_environment(input_data)
        status, data = run_pipeline(data_path=self.test_data_dir, enabled_checks={'WoW': False})
        
        self.assertEqual(status, "SUCCESS")
        self.assertFalse(any(r['Check_Type'] == 'WoW' for r in data['breach_report']))

    def test_output_csv_generation(self):
        """TEST: Verifies threshold_breaks.csv is created correctly."""
        input_data = {'DJIA': {'observation_date': ['2026-01-01', '2026-01-02'], 'closing_price': [100, 105]}}
        self.create_mock_environment(input_data)
        
        run_pipeline(data_path=self.test_data_dir)
        output_path = "./output/threshold_breaks.csv"
        self.assertTrue(os.path.exists(output_path))
        
        df_output = pd.read_csv(output_path)
        for col in ['Ticker', 'Date', 'Difference_Pct']:
            self.assertIn(col, df_output.columns)
    # --- CATEGORY 1: MARKET TIMING ---

    def test_weekend_gap(self):
        """TEST: Monday recognizes Friday as previous day."""
        input_data = {
            'DJIA': {'observation_date': ['2026-04-17', '2026-04-20'], 'closing_price': [38000.00, 38201.40]}
        }
        self.create_mock_environment(input_data)
        status, data = run_pipeline(data_path=self.test_data_dir)
        
        self.assertEqual(status, "SUCCESS")
        actual_change = data['processed_data']['DJIA'].iloc[-1]['daily_return']
        self.assertAlmostEqual(actual_change, 0.0053, places=4)
    def test_leap_year(self):
        """TEST: System accepts Feb 29 as a valid date."""
        input_data = {
            'SP500': {'observation_date': ['2028-02-28', '2028-02-29', '2028-03-01'], 'closing_price': [5000, 5010, 5020]}
        }
        self.create_mock_environment(input_data)
        status, data = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(len(data['processed_data']['SP500']), 3)

    def test_holiday_null_value(self):
        """TEST: Handles a date with a blank price (NaN)."""
        csv_content = "observation_date,closing_price\n2016-01-15,582.79\n2016-01-18,\n2016-01-19,591.87\n"
        with open(os.path.join(self.test_data_dir, "DJIA.csv"), "w") as f:
            f.write(csv_content)
        
        self.create_mock_environment({})
        status, data = run_pipeline(data_path=self.test_data_dir)
        
        self.assertEqual(status, "SUCCESS")
        actual_change = data['processed_data']['DJIA'].iloc[-1]['daily_return']
        self.assertAlmostEqual(actual_change, 0.0155, places=4)
    # --- CATEGORY 2: NUMERICAL ANOMALIES ---

    def test_negative_price(self):
        """TEST: REJECT negative prices."""
        input_data = {'DJTA': {'observation_date': ['2026-01-01'], 'closing_price': [-50.00]}}
        self.create_mock_environment(input_data)
        status, data = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Invalid Price")

    def test_zero_price(self):
        """TEST: Pipeline must REJECT zero prices."""
        input_data = {
            'DJUA': {'observation_date': ['2026-01-01'], 'closing_price': [0.00]}
        }
        self.create_mock_environment(input_data)
        status, data = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Invalid Price")

    def test_extreme_volatility(self):
        """TEST: WARNING on extreme volatility."""
        input_data = {'DJTA': {'observation_date': ['2026-01-03', '2026-01-04'], 'closing_price': [100.00, 10000.00]}}
        self.create_mock_environment(input_data)
        status, data = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "WARNING: Extreme Volatility")

    # --- CATEGORY 3: DATA INTEGRITY ---

    def test_sync_error(self):
        """TEST: Detects file date mismatch."""
        input_data = {
            'DJIA': {'observation_date': ['2026-05-01'], 'closing_price': [100]},
            'SP500': {'observation_date': ['2026-05-02'], 'closing_price': [100]}
        }
        self.create_mock_environment(input_data)
        status, data = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Files out of sync")

    def test_missing_files(self):
        """TEST: REJECT if required file is missing."""
        self.create_mock_environment({}, markets_to_create=['DJIA', 'SP500'])
        status, data = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Missing Files")

    def test_date_mismatch(self):
        """TEST: Rejects if dates are not in correct form : YYYY-MM-DD"""
        input_data = {
            'DJIA': {'observation_date': ['12-31-2026'], 'closing_price': [100]}
        }
        self.create_mock_environment(input_data)
        status, _ = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Date in wrong format")
    
    def test_random_string(self):
        """TEST: Rejects if any column has a random string in it."""
        input_data = {
            'DJIA': {'observation_date': ['Tuesday'], 'closing_price': [100]}
        }
        self.create_mock_environment(input_data)
        status, _ = run_pipeline(data_path=self.test_data_dir)
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
        status, _ = run_pipeline(data_path=self.test_data_dir)
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
        status, _ = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Duplicate Dates")
    #--ALL TESTS PAST THIS POINT WERE SUGGESTED BY GEMINI HAS MISSING FROM TESTING SUITE--
    def test_empty_file(self):
        """TEST: Rejects if file empty"""
        input_data = {
            'DJCA' : {
                'observation_date' : [],
                'closing_price':[]
            }
        }    
        self.create_mock_environment(input_data)
        status, _ = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: empty file")

    def test_header_mismatch(self):
        """TEST: REJECT if column name is incorrect."""
        df = pd.DataFrame({'Date': ['2026-01-01'], 'closing_price': [100]})
        df.to_csv(os.path.join(self.test_data_dir, "DJIA.csv"), index=False)
        self.create_mock_environment({'DJIA': df})
        status, data = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(status, "REJECT: Header mismatch")

    def test_trailing_whitespace_trim(self):
        """TEST: Whitespace is trimmed and parsed."""
        df = pd.DataFrame({'observation_date': ['2026-04-17 ', '2026-04-18'], 'closing_price': [' 38000.00', '38100.00 ']})
        df.to_csv(os.path.join(self.test_data_dir, "DJIA.csv"), index=False)
        self.create_mock_environment({})
        status, data = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(data['processed_data']['DJIA'].iloc[0]['observation_date'], '2026-04-17')
    def test_non_numeric_price(self):
        """TEST: Non-numeric placeholders N/A and '.' are rejected"""
        df = pd.DataFrame({
            'observation_date': ['2026-04-17', '2026-04-18'],
            'closing_price': ['N/A', '.']
        })
        df.to_csv(os.path.join(self.test_data_dir, "SP500.csv"), index=False)
        self.create_mock_environment({})  # populate other required market files
        
        status, _ = run_pipeline(data_path=self.test_data_dir)
        
        # Using assertIn because "Missing Price" might be part of a larger error string
        self.assertIn("REJECT: Missing Price", status)

    def test_extreme_precision(self):
        """TEST: Rounding to 2 decimals."""
        df = pd.DataFrame({'observation_date': ['2026-04-17'], 'closing_price': [38000.1234567]})
        df.to_csv(os.path.join(self.test_data_dir, "DJIA.csv"), index=False)
        self.create_mock_environment({})
        status, data = run_pipeline(data_path=self.test_data_dir)
        self.assertAlmostEqual(data['processed_data']['DJIA'].iloc[0]['closing_price'], 38000.12, places=2)   

    def test_range_mismatch(self):
        """TEST: Detects incomplete overlap when market date ranges start at different times."""
        input_data = {
            'DJIA': {
                'observation_date': ['2020-01-01', '2020-01-02'],
                'closing_price': [30000.0, 30100.0]
            },
            'SP500': {
                'observation_date': ['2022-01-01', '2022-01-02'], # Fixed the missing '2' in your date string
                'closing_price': [4700.0, 4710.0]
            }
        }
        self.create_mock_environment(input_data)
        
        status, _ = run_pipeline(data_path=self.test_data_dir)
        
        # Checks if either WARNING or REJECT is returned based on your implementation choice
        self.assertTrue(any(msg in status for msg in ["WARNING: Incomplete overlap", "REJECT: Incomplete overlap"]))

    def test_comma_formatting(self):
        """TEST: Comma in price is parsed as float."""
        df = pd.DataFrame({'observation_date': ['2026-01-01'], 'closing_price': ['38,000.00']})
        df.to_csv(os.path.join(self.test_data_dir, "DJIA.csv"), index=False)
        self.create_mock_environment({'DJIA': df})
        status, data = run_pipeline(data_path=self.test_data_dir)
        self.assertEqual(data['processed_data']['DJIA'].iloc[0]['closing_price'], 38000.00)
if __name__ == '__main__':
    unittest.main()