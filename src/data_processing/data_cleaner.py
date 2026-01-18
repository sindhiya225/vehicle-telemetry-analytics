import pandas as pd
import numpy as np
from typing import Dict, Tuple, List
import re

class VehicleDataCleaner:
    def __init__(self, file_path: str):
        self.df = pd.read_excel(file_path)
        self.cleaned_df = None
        
    def clean_data(self) -> pd.DataFrame:
        """Main cleaning pipeline"""
        self._standardize_columns()
        self._handle_missing_values()
        self._extract_engine_features()
        self._parse_transmission()
        self._normalize_weight()
        self._categorize_vehicles()
        self._validate_data()
        
        self.cleaned_df = self.df.copy()
        return self.cleaned_df
    
    def _standardize_columns(self):
        """Standardize column names"""
        self.df.columns = [col.strip().lower().replace(' ', '_') 
                          for col in self.df.columns]
    
    def _handle_missing_values(self):
        """Handle NO DATA entries"""
        self.df.replace('NO DATA', np.nan, inplace=True)
        
        # Fill vehicle_class based on weight
        weight_class_map = {
            (0, 3000): 'Compact',
            (3000, 4000): 'Midsize',
            (4000, 5000): 'SUV',
            (5000, 7000): 'Truck',
            (7000, 10000): 'Heavy Duty'
        }
        
        def classify_by_weight(weight):
            if pd.isna(weight):
                return 'Unknown'
            for (min_w, max_w), vclass in weight_class_map.items():
                if min_w <= weight < max_w:
                    return vclass
            return 'Heavy Duty'
        
        mask = self.df['vehicle_class'].isna()
        self.df.loc[mask, 'vehicle_class'] = self.df.loc[mask, 'generalized_weight'].apply(
            classify_by_weight
        )
    
    def _extract_engine_features(self):
        """Extract detailed engine features"""
        
        def parse_engine_config(config):
            if pd.isna(config):
                return {
                    'engine_cylinders': np.nan,
                    'engine_type': np.nan,
                    'displacement_l': np.nan,
                    'is_turbo': False,
                    'is_hybrid': False
                }
            
            # Extract cylinders
            cyl_match = re.search(r'(\d+)[-\s]', str(config))
            cylinders = int(cyl_match.group(1)) if cyl_match else np.nan
            
            # Extract displacement
            disp_match = re.search(r'(\d+\.?\d*)\s*L', str(config), re.IGNORECASE)
            displacement = float(disp_match.group(1)) if disp_match else np.nan
            
            # Determine engine type
            is_hybrid = 'GAS/ELECTRIC' in str(config) or 'ELECTRIC' in str(config)
            is_turbo = 'T/C' in str(config) or 'TURBO' in str(config)
            
            engine_type = 'Hybrid' if is_hybrid else (
                'Gasoline' if 'GAS' in str(config) else (
                    'Diesel' if 'DSL' in str(config) else 'Gasoline'
                )
            )
            
            return {
                'engine_cylinders': cylinders,
                'engine_type': engine_type,
                'displacement_l': displacement,
                'is_turbo': is_turbo,
                'is_hybrid': is_hybrid
            }
        
        # Apply parsing
        engine_features = self.df['engine_configuration_&_displacement'].apply(parse_engine_config)
        
        # Create new columns
        self.df['engine_cylinders'] = engine_features.apply(lambda x: x['engine_cylinders'])
        self.df['engine_type'] = engine_features.apply(lambda x: x['engine_type'])
        self.df['displacement_l'] = engine_features.apply(lambda x: x['displacement_l'])
        self.df['is_turbo'] = engine_features.apply(lambda x: x['is_turbo'])
        self.df['is_hybrid'] = engine_features.apply(lambda x: x['is_hybrid'])
        
        # Calculate engine power index (simplified)
        self.df['power_index'] = self.df['displacement_l'] * self.df['engine_cylinders'] * \
                                (1.5 if self.df['is_turbo'] else 1.0)
    
    def _parse_transmission(self):
        """Parse transmission details"""
        
        def classify_transmission(trans):
            if pd.isna(trans):
                return 'Unknown'
            
            trans_str = str(trans).upper()
            
            if 'MANUAL' in trans_str:
                return 'Manual'
            elif 'AUTOMATIC' in trans_str:
                if 'CVT' in trans_str:
                    return 'CVT'
                elif 'ECT' in trans_str:
                    return 'Automatic (ECT)'
                else:
                    return 'Automatic'
            elif 'CVT' in trans_str:
                return 'CVT'
            else:
                return 'Other'
        
        def extract_speeds(trans):
            if pd.isna(trans):
                return np.nan
            
            speed_match = re.search(r'(\d+)[-\s]*SP', str(trans))
            return int(speed_match.group(1)) if speed_match else np.nan
        
        self.df['transmission_type'] = self.df['transmission'].apply(classify_transmission)
        self.df['transmission_speeds'] = self.df['transmission'].apply(extract_speeds)
    
    def _normalize_weight(self):
        """Normalize weight and calculate weight class"""
        # Fill missing weights with median based on vehicle type
        for vtype in self.df['vehicle_type'].unique():
            median_weight = self.df[self.df['vehicle_type'] == vtype]['generalized_weight'].median()
            mask = (self.df['vehicle_type'] == vtype) & (self.df['generalized_weight'].isna())
            self.df.loc[mask, 'generalized_weight'] = median_weight
        
        # Create weight categories
        bins = [0, 2500, 3500, 4500, 5500, 10000]
        labels = ['Light', 'Medium-Light', 'Medium', 'Medium-Heavy', 'Heavy']
        self.df['weight_category'] = pd.cut(
            self.df['generalized_weight'], 
            bins=bins, 
            labels=labels, 
            include_lowest=True
        )
    
    def _categorize_vehicles(self):
        """Create additional vehicle categories"""
        # Fuel efficiency category based on displacement and weight
        self.df['efficiency_score'] = self.df['generalized_weight'] / (self.df['displacement_l'] + 0.1)
        
        bins = [0, 800, 1200, 2000, 5000]
        labels = ['Low', 'Medium', 'High', 'Very High']
        self.df['efficiency_category'] = pd.cut(
            self.df['efficiency_score'], 
            bins=bins, 
            labels=labels
        )
    
    def _validate_data(self):
        """Validate cleaned data"""
        # Remove duplicates
        self.df = self.df.drop_duplicates(subset=['vehid'])
        
        # Validate ranges
        assert self.df['displacement_l'].between(0.5, 8.0).all() or \
               self.df['displacement_l'].isna().any(), "Displacement out of range"
        
        assert self.df['generalized_weight'].between(1000, 10000).all() or \
               self.df['generalized_weight'].isna().any(), "Weight out of range"
    
    def save_cleaned_data(self, output_path: str):
        """Save cleaned data to CSV"""
        if self.cleaned_df is not None:
            self.cleaned_df.to_csv(output_path, index=False)
            print(f"Cleaned data saved to {output_path}")