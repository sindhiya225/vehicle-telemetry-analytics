# dashboard/tableau/generate_tableau_workbook.py
import pandas as pd
import tableauserverclient as TSC
from datetime import datetime
import json

class TableauDashboardGenerator:
    def __init__(self, server_url, username, password, site_id=''):
        """Initialize Tableau Server connection"""
        self.server = TSC.Server(server_url)
        self.server.version = '3.12'
        self.auth = TSC.TableauAuth(username, password, site_id)
        
    def create_workbook_from_data(self, dataframes_dict, workbook_name):
        """Create Tableau workbook from multiple dataframes"""
        
        # Create a TDE file (Tableau Data Extract)
        import tableausdk.Extract as tde
        
        # Create extract
        extract_path = f"{workbook_name}.tde"
        extract = tde.Extract(extract_path)
        
        # Define table definition
        table_def = tde.TableDefinition()
        
        # Add columns based on first dataframe
        first_df_name = list(dataframes_dict.keys())[0]
        first_df = dataframes_dict[first_df_name]
        
        for col in first_df.columns:
            if first_df[col].dtype == 'object':
                table_def.addColumn(col, tde.Type.CHAR_STRING)
            elif first_df[col].dtype in ['int64', 'int32']:
                table_def.addColumn(col, tde.Type.INTEGER)
            elif first_df[col].dtype in ['float64', 'float32']:
                table_def.addColumn(col, tde.Type.DOUBLE)
            elif first_df[col].dtype == 'bool':
                table_def.addColumn(col, tde.Type.BOOLEAN)
            elif first_df[col].dtype == 'datetime64[ns]':
                table_def.addColumn(col, tde.Type.DATETIME)
            else:
                table_def.addColumn(col, tde.Type.CHAR_STRING)
        
        # Create table in extract
        table = extract.addTable("Extract", table_def)
        
        # Add rows
        new_row = tde.Row(table_def)
        for _, row in first_df.iterrows():
            for i, col in enumerate(first_df.columns):
                value = row[col]
                if pd.isna(value):
                    new_row.setNull(i)
                elif isinstance(value, str):
                    new_row.setCharString(i, str(value))
                elif isinstance(value, (int, np.integer)):
                    new_row.setInteger(i, int(value))
                elif isinstance(value, (float, np.floating)):
                    new_row.setDouble(i, float(value))
                elif isinstance(value, bool):
                    new_row.setBoolean(i, bool(value))
                elif isinstance(value, pd.Timestamp):
                    new_row.setDateTime(i, value.year, value.month, value.day, 
                                       value.hour, value.minute, value.second, 0)
            table.insert(new_row)
        
        extract.close()
        
        print(f"Created TDE extract: {extract_path}")
        return extract_path
    
    def publish_workbook(self, extract_path, workbook_name, project_id=None):
        """Publish workbook to Tableau Server"""
        with self.server.auth.sign_in(self.auth):
            # Create workbook item
            workbook_item = TSC.WorkbookItem(
                name=workbook_name,
                project_id=project_id or ''
            )
            
            # Publish workbook
            workbook_item = self.server.workbooks.publish(
                workbook_item,
                extract_path,
                mode=TSC.Server.PublishMode.Overwrite
            )
            
            print(f"Published workbook: {workbook_item.name}")
            print(f"Workbook ID: {workbook_item.id}")
            print(f"View URL: {workbook_item.webpage_url}")
            
            return workbook_item
    
    def create_datasource_connections(self, db_config):
        """Create data source connections for the workbook"""
        connections = []
        
        # PostgreSQL connection
        postgres_conn = {
            'class': 'postgres',
            'dbname': db_config.get('database', 'vehicle_telemetry'),
            'server': db_config.get('host', 'localhost'),
            'port': db_config.get('port', 5432),
            'username': db_config.get('username', 'postgres'),
            'password': db_config.get('password', ''),
            'initial_sql': """
                -- Initial SQL to set up the connection
                SET search_path TO public;
            """
        }
        connections.append(('PostgreSQL', postgres_conn))
        
        # CSV connection (for static data)
        csv_conn = {
            'class': 'textscan',
            'directory': './data/processed/',
            'files': ['cleaned_vehicle_data.csv']
        }
        connections.append(('CSV Files', csv_conn))
        
        return connections
    
    def generate_tableau_hyper(self, dataframe, filename):
        """Generate Tableau Hyper file from pandas DataFrame"""
        from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry
        from tableauhyperapi import Inserter, CreateMode, NOT_NULLABLE
        
        with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, 'myapp') as hyper:
            with Connection(hyper.endpoint, filename, CreateMode.CREATE_AND_REPLACE) as connection:
                # Define schema based on dataframe
                schema = TableDefinition(table_name='Extract')
                
                for col_name, col_type in dataframe.dtypes.items():
                    if col_type == 'object':
                        sql_type = SqlType.text()
                    elif col_type in ['int64', 'int32']:
                        sql_type = SqlType.big_int()
                    elif col_type in ['float64', 'float32']:
                        sql_type = SqlType.double()
                    elif col_type == 'bool':
                        sql_type = SqlType.bool()
                    elif col_type == 'datetime64[ns]':
                        sql_type = SqlType.timestamp()
                    else:
                        sql_type = SqlType.text()
                    
                    schema.add_column(col_name, sql_type, NOT_NULLABLE)
                
                # Create table
                connection.catalog.create_table(schema)
                
                # Insert data
                with Inserter(connection, schema) as inserter:
                    for _, row in dataframe.iterrows():
                        row_data = []
                        for col in dataframe.columns:
                            value = row[col]
                            if pd.isna(value):
                                row_data.append(None)
                            else:
                                row_data.append(value)
                        inserter.add_row(row_data)
                    inserter.execute()
        
        print(f"Created Hyper file: {filename}")
        return filename

# Alternative: Generate Tableau workbook using Python (if Tableau Server not available)
def generate_tableau_dashboard_code():
    """Generate Tableau dashboard configuration as code"""
    
    dashboard_config = {
        "name": "Vehicle Telemetry Analytics",
        "version": "1.0",
        "created": datetime.now().isoformat(),
        "data_sources": [
            {
                "name": "vehicle_telemetry",
                "type": "postgresql",
                "connection": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "vehicle_telemetry",
                    "username": "postgres"
                },
                "tables": [
                    "vehicles",
                    "telemetry_data",
                    "daily_aggregations",
                    "maintenance_history"
                ]
            }
        ],
        "worksheets": [
            {
                "name": "Fuel Efficiency Analysis",
                "data_source": "vehicle_telemetry",
                "fields": {
                    "dimensions": ["vehicle_type", "vehicle_class", "engine_type"],
                    "measures": ["fuel_efficiency_kmpl", "trip_distance_km", "fuel_consumed_l"],
                    "calculated_fields": [
                        {
                            "name": "Cost per KM",
                            "formula": "[fuel_consumed_l] * 1.2 / [trip_distance_km]"
                        },
                        {
                            "name": "Efficiency Score",
                            "formula": "([fuel_efficiency_kmpl] / 20) * 100"
                        }
                    ]
                },
                "charts": [
                    {
                        "type": "bar",
                        "x": "vehicle_type",
                        "y": "AVG([fuel_efficiency_kmpl])",
                        "color": "vehicle_class"
                    },
                    {
                        "type": "line",
                        "x": "DATE([timestamp])",
                        "y": "AVG([fuel_efficiency_kmpl])",
                        "color": "vehicle_type"
                    },
                    {
                        "type": "scatter",
                        "x": "trip_distance_km",
                        "y": "fuel_efficiency_kmpl",
                        "color": "vehicle_type",
                        "size": "fuel_consumed_l"
                    }
                ]
            },
            {
                "name": "Maintenance Dashboard",
                "data_source": "vehicle_telemetry",
                "fields": {
                    "dimensions": ["vehid", "vehicle_type", "has_fault", "maintenance_flag"],
                    "measures": ["fault_count", "maintenance_count"],
                    "calculated_fields": [
                        {
                            "name": "Maintenance Urgency",
                            "formula": """
                                CASE [fault_count]
                                    WHEN 0 THEN 'Low'
                                    WHEN 1 THEN 'Medium'
                                    ELSE 'High'
                                END
                            """
                        }
                    ]
                },
                "charts": [
                    {
                        "type": "heatmap",
                        "x": "vehicle_type",
                        "y": "SUM([fault_count])",
                        "color": "Maintenance Urgency"
                    },
                    {
                        "type": "treemap",
                        "size": "COUNT([vehid])",
                        "color": "AVG([fault_count])",
                        "label": "vehicle_class"
                    }
                ]
            },
            {
                "name": "Cost Analysis",
                "data_source": "vehicle_telemetry",
                "fields": {
                    "dimensions": ["vehicle_class", "engine_type", "weight_category"],
                    "measures": ["total_cost", "fuel_cost", "maintenance_cost"],
                    "calculated_fields": [
                        {
                            "name": "Cost per 100km",
                            "formula": "([total_cost] / SUM([trip_distance_km])) * 100"
                        },
                        {
                            "name": "Cost Savings Potential",
                            "formula": "[total_cost] * 0.15"
                        }
                    ]
                },
                "charts": [
                    {
                        "type": "bar",
                        "x": "vehicle_class",
                        "y": "SUM([total_cost])",
                        "color": "engine_type"
                    },
                    {
                        "type": "bullet",
                        "value": "SUM([total_cost])",
                        "comparative": "SUM([Cost Savings Potential])",
                        "label": "vehicle_class"
                    }
                ]
            },
            {
                "name": "Driver Behavior Analysis",
                "data_source": "vehicle_telemetry",
                "fields": {
                    "dimensions": ["vehid", "driver_category"],
                    "measures": ["driver_behavior_score", "harsh_braking_count", "speeding_count"],
                    "calculated_fields": [
                        {
                            "name": "Safety Score",
                            "formula": "100 - ([harsh_braking_count] * 10 + [speeding_count] * 5)"
                        }
                    ]
                },
                "charts": [
                    {
                        "type": "gauge",
                        "value": "AVG([driver_behavior_score])",
                        "min": 0,
                        "max": 1
                    },
                    {
                        "type": "histogram",
                        "value": "driver_behavior_score",
                        "bins": 20
                    }
                ]
            }
        ],
        "dashboards": [
            {
                "name": "Executive Summary",
                "layout": [
                    {
                        "type": "kpi",
                        "title": "Total Vehicles",
                        "value": "COUNTD([vehid])",
                        "position": {"row": 1, "col": 1, "width": 2, "height": 1}
                    },
                    {
                        "type": "kpi",
                        "title": "Avg Efficiency",
                        "value": "AVG([fuel_efficiency_kmpl])",
                        "position": {"row": 1, "col": 3, "width": 2, "height": 1}
                    },
                    {
                        "type": "kpi",
                        "title": "Total Faults",
                        "value": "SUM([fault_count])",
                        "position": {"row": 1, "col": 5, "width": 2, "height": 1}
                    },
                    {
                        "type": "chart",
                        "worksheet": "Fuel Efficiency Analysis",
                        "chart_index": 0,
                        "position": {"row": 2, "col": 1, "width": 4, "height": 3}
                    },
                    {
                        "type": "chart",
                        "worksheet": "Maintenance Dashboard",
                        "chart_index": 0,
                        "position": {"row": 2, "col": 5, "width": 3, "height": 3}
                    }
                ]
            },
            {
                "name": "Cost Optimization",
                "layout": [
                    {
                        "type": "chart",
                        "worksheet": "Cost Analysis",
                        "chart_index": 0,
                        "position": {"row": 1, "col": 1, "width": 6, "height": 4}
                    },
                    {
                        "type": "chart",
                        "worksheet": "Cost Analysis",
                        "chart_index": 1,
                        "position": {"row": 1, "col": 7, "width": 5, "height": 4}
                    }
                ]
            }
        ]
    }
    
    # Save as JSON for documentation
    with open('tableau_dashboard_config.json', 'w') as f:
        json.dump(dashboard_config, f, indent=2)
    
    return dashboard_config

if __name__ == "__main__":
    # Generate Tableau configuration
    config = generate_tableau_dashboard_code()
    print("Tableau dashboard configuration generated")
    print(f"Saved to: tableau_dashboard_config.json")
    
    # For actual Tableau Server publishing (uncomment if you have Tableau Server)
    # generator = TableauDashboardGenerator(
    #     server_url='http://localhost',
    #     username='admin',
    #     password='password'
    # )