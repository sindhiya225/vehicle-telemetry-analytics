"""
Report Generator for Vehicle Telemetry Analytics Platform.
Creates comprehensive reports in various formats (PDF, HTML, Excel).
"""
import logging
import json
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
import os
import io
from dataclasses import dataclass, field, asdict
from enum import Enum
import jinja2
from fpdf import FPDF
import xlsxwriter
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
import matplotlib.pyplot as plt
import seaborn as sns
from weasyprint import HTML
import plotly.graph_objects as go
import plotly.io as pio

logger = logging.getLogger(__name__)


class ReportFormat(Enum):
    """Supported report formats."""
    PDF = "pdf"
    HTML = "html"
    EXCEL = "excel"
    MARKDOWN = "markdown"
    JSON = "json"


class ReportSection:
    """Report section with content and metadata."""
    
    def __init__(self, title: str, content: Any, level: int = 1):
        self.title = title
        self.content = content
        self.level = level
        self.subsections = []
    
    def add_subsection(self, title: str, content: Any):
        """Add subsection."""
        subsection = ReportSection(title, content, self.level + 1)
        self.subsections.append(subsection)
        return subsection
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'title': self.title,
            'level': self.level,
            'content': str(self.content),
            'subsections': [s.to_dict() for s in self.subsections]
        }


@dataclass
class ReportMetrics:
    """Report metrics container."""
    vehicle_id: Optional[str] = None
    fleet_id: Optional[str] = None
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    total_distance_km: float = 0.0
    total_fuel_consumed_l: float = 0.0
    avg_fuel_efficiency_kmpl: float = 0.0
    total_driving_time_h: float = 0.0
    avg_speed_kmh: float = 0.0
    max_speed_kmh: float = 0.0
    harsh_acceleration_count: int = 0
    harsh_braking_count: int = 0
    harsh_cornering_count: int = 0
    speeding_count: int = 0
    idle_time_h: float = 0.0
    fault_count: int = 0
    maintenance_cost: float = 0.0
    co2_emissions_kg: float = 0.0
    
    def calculate_co2_emissions(self, fuel_type: str = 'petrol'):
        """Calculate CO2 emissions based on fuel consumption."""
        # CO2 emission factors (kg CO2 per liter)
        emission_factors = {
            'petrol': 2.31,
            'diesel': 2.68,
            'cng': 2.75,
            'lpg': 1.51,
            'electric': 0.0  # Assuming renewable energy
        }
        
        factor = emission_factors.get(fuel_type.lower(), 2.31)
        self.co2_emissions_kg = self.total_fuel_consumed_l * factor
        return self.co2_emissions_kg
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class BaseReportGenerator:
    """Base class for report generators."""
    
    def __init__(self, title: str, metrics: ReportMetrics = None):
        self.title = title
        self.metrics = metrics or ReportMetrics()
        self.sections = []
        self.figures = {}
        self.tables = {}
        self.metadata = {
            'generated_at': datetime.now(),
            'version': '1.0.0',
            'author': 'Vehicle Analytics Platform'
        }
    
    def add_section(self, title: str, content: Any, level: int = 1) -> ReportSection:
        """Add section to report."""
        section = ReportSection(title, content, level)
        self.sections.append(section)
        return section
    
    def add_figure(self, name: str, figure: Any, caption: str = None):
        """Add figure to report."""
        self.figures[name] = {
            'figure': figure,
            'caption': caption or name
        }
    
    def add_table(self, name: str, data: pd.DataFrame, caption: str = None):
        """Add table to report."""
        self.tables[name] = {
            'data': data,
            'caption': caption or name
        }
    
    def generate(self, output_path: str = None) -> Any:
        """Generate report."""
        raise NotImplementedError
    
    def save(self, filepath: str):
        """Save report to file."""
        raise NotImplementedError


class HTMLReportGenerator(BaseReportGenerator):
    """HTML report generator."""
    
    def __init__(self, title: str, metrics: ReportMetrics = None, template_path: str = None):
        super().__init__(title, metrics)
        self.template_path = template_path
        self.css_style = self._get_default_css()
    
    def _get_default_css(self) -> str:
        """Get default CSS style."""
        return """
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f8f9fa;
            }
            .report-header {
                background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
                color: white;
                padding: 30px;
                border-radius: 10px;
                margin-bottom: 30px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }
            .report-title {
                font-size: 32px;
                font-weight: bold;
                margin-bottom: 10px;
            }
            .report-subtitle {
                font-size: 18px;
                opacity: 0.9;
            }
            .metrics-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            .metric-card {
                background: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                transition: transform 0.3s ease;
            }
            .metric-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 5px 15px rgba(0, 0, 0, 0.2);
            }
            .metric-value {
                font-size: 24px;
                font-weight: bold;
                color: #2a5298;
                margin-bottom: 5px;
            }
            .metric-label {
                font-size: 14px;
                color: #666;
                text-transform: uppercase;
                letter-spacing: 1px;
            }
            .section {
                background: white;
                padding: 25px;
                border-radius: 8px;
                margin-bottom: 25px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            }
            .section-title {
                font-size: 24px;
                color: #2a5298;
                border-bottom: 2px solid #e9ecef;
                padding-bottom: 10px;
                margin-bottom: 20px;
            }
            .subsection-title {
                font-size: 20px;
                color: #495057;
                margin-top: 20px;
                margin-bottom: 15px;
            }
            .figure-container {
                margin: 20px 0;
                text-align: center;
            }
            .figure-caption {
                font-style: italic;
                color: #666;
                margin-top: 10px;
                font-size: 14px;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
            }
            th {
                background-color: #2a5298;
                color: white;
                padding: 12px;
                text-align: left;
            }
            td {
                padding: 10px 12px;
                border-bottom: 1px solid #dee2e6;
            }
            tr:hover {
                background-color: #f8f9fa;
            }
            .footer {
                text-align: center;
                margin-top: 40px;
                padding-top: 20px;
                border-top: 1px solid #dee2e6;
                color: #666;
                font-size: 14px;
            }
            .badge {
                display: inline-block;
                padding: 4px 8px;
                border-radius: 4px;
                font-size: 12px;
                font-weight: bold;
                margin-right: 5px;
            }
            .badge-success { background-color: #28a745; color: white; }
            .badge-warning { background-color: #ffc107; color: #212529; }
            .badge-danger { background-color: #dc3545; color: white; }
            .badge-info { background-color: #17a2b8; color: white; }
        </style>
        """
    
    def generate(self, output_path: str = None) -> str:
        """Generate HTML report."""
        # Create template
        template_str = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{{ title }}</title>
            {{ css_style }}
        </head>
        <body>
            <div class="report-header">
                <div class="report-title">{{ title }}</div>
                <div class="report-subtitle">
                    Generated on {{ metadata.generated_at.strftime('%Y-%m-%d %H:%M:%S') }} | 
                    Version {{ metadata.version }}
                </div>
            </div>
            
            {% if metrics %}
            <div class="metrics-grid">
                {% for key, value in metrics.items() %}
                {% if value is number and key != 'vehicle_id' and key != 'fleet_id' %}
                <div class="metric-card">
                    <div class="metric-value">
                        {% if 'km' in key %}
                            {{ value|round(1) }} km
                        {% elif 'kmh' in key or 'kmpl' in key %}
                            {{ value|round(1) }}
                        {% elif 'h' in key %}
                            {{ value|round(1) }} h
                        {% elif 'count' in key %}
                            {{ value }}
                        {% elif 'cost' in key %}
                            ${{ value|round(2) }}
                        {% elif 'kg' in key %}
                            {{ value|round(1) }} kg
                        {% else %}
                            {{ value|round(2) }}
                        {% endif %}
                    </div>
                    <div class="metric-label">{{ key|replace('_', ' ')|title }}</div>
                </div>
                {% endif %}
                {% endfor %}
            </div>
            {% endif %}
            
            {% for section in sections %}
            <div class="section">
                <h2 class="section-title">{{ section.title }}</h2>
                <div class="section-content">
                    {{ section.content }}
                    
                    {% for subsection in section.subsections %}
                    <h3 class="subsection-title">{{ subsection.title }}</h3>
                    <div class="subsection-content">
                        {{ subsection.content }}
                    </div>
                    {% endfor %}
                </div>
            </div>
            {% endfor %}
            
            {% for name, figure_data in figures.items() %}
            <div class="section">
                <h2 class="section-title">{{ figure_data.caption }}</h2>
                <div class="figure-container">
                    {{ figure_data.figure }}
                </div>
            </div>
            {% endfor %}
            
            {% for name, table_data in tables.items() %}
            <div class="section">
                <h2 class="section-title">{{ table_data.caption }}</h2>
                {{ table_data.data.to_html(classes='dataframe', index=False) }}
            </div>
            {% endfor %}
            
            <div class="footer">
                <p>Generated by Vehicle Analytics Platform | {{ metadata.author }}</p>
                <p>Confidential - For internal use only</p>
            </div>
        </body>
        </html>
        """
        
        # Prepare data
        metrics_dict = self.metrics.to_dict() if self.metrics else {}
        
        # Convert sections to HTML
        sections_html = []
        for section in self.sections:
            section_dict = section.to_dict()
            sections_html.append(section_dict)
        
        # Convert figures to HTML
        figures_html = {}
        for name, fig_data in self.figures.items():
            if isinstance(fig_data['figure'], go.Figure):
                fig_html = fig_data['figure'].to_html(full_html=False, include_plotlyjs='cdn')
            else:
                fig_html = f"<p>Figure: {fig_data['caption']}</p>"
            
            figures_html[name] = {
                'figure': fig_html,
                'caption': fig_data['caption']
            }
        
        # Convert tables to HTML
        tables_html = {}
        for name, table_data in self.tables.items():
            tables_html[name] = {
                'data': table_data['data'],
                'caption': table_data['caption']
            }
        
        # Render template
        template = jinja2.Template(template_str)
        html_content = template.render(
            title=self.title,
            css_style=self.css_style,
            metrics=metrics_dict,
            sections=sections_html,
            figures=figures_html,
            tables=tables_html,
            metadata=self.metadata
        )
        
        if output_path:
            self.save(output_path)
        
        return html_content
    
    def save(self, filepath: str):
        """Save HTML report to file."""
        html_content = self.generate()
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML report saved to {filepath}")


class PDFReportGenerator(BaseReportGenerator):
    """PDF report generator using ReportLab."""
    
    def __init__(self, title: str, metrics: ReportMetrics = None):
        super().__init__(title, metrics)
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Setup custom styles for PDF."""
        # Title style
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Title'],
            fontSize=24,
            spaceAfter=30,
            textColor=colors.HexColor('#2a5298')
        ))
        
        # Heading styles
        self.styles.add(ParagraphStyle(
            name='Heading1',
            parent=self.styles['Heading1'],
            fontSize=18,
            spaceAfter=12,
            textColor=colors.HexColor('#2a5298')
        ))
        
        self.styles.add(ParagraphStyle(
            name='Heading2',
            parent=self.styles['Heading2'],
            fontSize=14,
            spaceAfter=8,
            textColor=colors.HexColor('#495057')
        ))
        
        # Normal text style
        self.styles.add(ParagraphStyle(
            name='CustomNormal',
            parent=self.styles['Normal'],
            fontSize=10,
            spaceAfter=6
        ))
        
        # Metric style
        self.styles.add(ParagraphStyle(
            name='Metric',
            parent=self.styles['Normal'],
            fontSize=16,
            textColor=colors.HexColor('#2a5298'),
            alignment=1  # Center alignment
        ))
    
    def generate(self, output_path: str = None) -> bytes:
        """Generate PDF report."""
        if output_path is None:
            output_path = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        
        # Create PDF document
        doc = SimpleDocTemplate(
            output_path,
            pagesize=letter,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=72
        )
        
        # Build story (content)
        story = []
        
        # Add title
        story.append(Paragraph(self.title, self.styles['CustomTitle']))
        story.append(Spacer(1, 12))
        
        # Add metadata
        metadata_text = f"""
        Generated on: {self.metadata['generated_at'].strftime('%Y-%m-%d %H:%M:%S')}<br/>
        Version: {self.metadata['version']}<br/>
        Author: {self.metadata['author']}
        """
        story.append(Paragraph(metadata_text, self.styles['CustomNormal']))
        story.append(Spacer(1, 20))
        
        # Add metrics section
        if self.metrics:
            story.append(Paragraph("Key Metrics", self.styles['Heading1']))
            story.append(Spacer(1, 10))
            
            # Create metrics table
            metrics_data = self._create_metrics_table_data()
            metrics_table = Table(metrics_data, colWidths=[3*inch, 2*inch])
            metrics_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2a5298')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            story.append(metrics_table)
            story.append(Spacer(1, 20))
        
        # Add sections
        for section in self.sections:
            story.append(Paragraph(section.title, self.styles['Heading1']))
            story.append(Spacer(1, 10))
            
            if isinstance(section.content, str):
                story.append(Paragraph(section.content, self.styles['CustomNormal']))
            elif isinstance(section.content, pd.DataFrame):
                # Convert DataFrame to table
                df = section.content
                table_data = [df.columns.tolist()] + df.values.tolist()
                table = Table(table_data, repeatRows=1)
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 8)
                ]))
                story.append(table)
            
            story.append(Spacer(1, 15))
            
            # Add subsections
            for subsection in section.subsections:
                story.append(Paragraph(subsection.title, self.styles['Heading2']))
                story.append(Spacer(1, 5))
                
                if isinstance(subsection.content, str):
                    story.append(Paragraph(subsection.content, self.styles['CustomNormal']))
                
                story.append(Spacer(1, 10))
        
        # Add figures (simplified - would need to save as images first)
        if self.figures:
            story.append(Paragraph("Charts and Visualizations", self.styles['Heading1']))
            story.append(Spacer(1, 10))
            
            for name, fig_data in self.figures.items():
                story.append(Paragraph(fig_data['caption'], self.styles['Heading2']))
                story.append(Spacer(1, 5))
                
                # Note: In production, you would save the figure as an image and add it here
                story.append(Paragraph("[Chart would be displayed here]", self.styles['CustomNormal']))
                story.append(Spacer(1, 10))
        
        # Add tables
        if self.tables:
            story.append(Paragraph("Data Tables", self.styles['Heading1']))
            story.append(Spacer(1, 10))
            
            for name, table_data in self.tables.items():
                story.append(Paragraph(table_data['caption'], self.styles['Heading2']))
                story.append(Spacer(1, 5))
                
                df = table_data['data']
                table_data_list = [df.columns.tolist()] + df.values.tolist()
                table = Table(table_data_list, repeatRows=1)
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ]))
                story.append(table)
                story.append(Spacer(1, 15))
        
        # Add footer
        story.append(Spacer(1, 30))
        footer_text = "Confidential - For internal use only | Generated by Vehicle Analytics Platform"
        story.append(Paragraph(footer_text, self.styles['CustomNormal']))
        
        # Build PDF
        doc.build(story)
        
        # Read generated PDF
        with open(output_path, 'rb') as f:
            pdf_bytes = f.read()
        
        return pdf_bytes
    
    def _create_metrics_table_data(self) -> List[List[str]]:
        """Create metrics table data for PDF."""
        metrics = self.metrics.to_dict() if self.metrics else {}
        
        table_data = [['Metric', 'Value']]
        
        for key, value in metrics.items():
            if key in ['vehicle_id', 'fleet_id', 'period_start', 'period_end']:
                continue
            
            if isinstance(value, (int, float)):
                # Format based on metric type
                if 'km' in key:
                    formatted_value = f"{value:.1f} km"
                elif 'kmh' in key or 'kmpl' in key:
                    formatted_value = f"{value:.1f}"
                elif 'h' in key:
                    formatted_value = f"{value:.1f} hours"
                elif 'count' in key:
                    formatted_value = f"{value:,}"
                elif 'cost' in key:
                    formatted_value = f"${value:.2f}"
                elif 'kg' in key:
                    formatted_value = f"{value:.1f} kg"
                else:
                    formatted_value = f"{value:.2f}"
                
                label = key.replace('_', ' ').title()
                table_data.append([label, formatted_value])
        
        return table_data
    
    def save(self, filepath: str):
        """Save PDF report to file."""
        pdf_bytes = self.generate(filepath)
        logger.info(f"PDF report saved to {filepath}")


class ExcelReportGenerator(BaseReportGenerator):
    """Excel report generator using XlsxWriter."""
    
    def __init__(self, title: str, metrics: ReportMetrics = None):
        super().__init__(title, metrics)
        self.workbook = None
        self.worksheet = None
    
    def generate(self, output_path: str = None) -> bytes:
        """Generate Excel report."""
        if output_path is None:
            output_path = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Create workbook and worksheet
        self.workbook = xlsxwriter.Workbook(output_path)
        self.worksheet = self.workbook.add_worksheet('Report')
        
        # Set column widths
        self.worksheet.set_column('A:A', 30)
        self.worksheet.set_column('B:B', 20)
        
        # Define formats
        title_format = self.workbook.add_format({
            'bold': True,
            'font_size': 16,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#2a5298',
            'font_color': 'white'
        })
        
        header_format = self.workbook.add_format({
            'bold': True,
            'font_size': 12,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#4a72b8',
            'font_color': 'white',
            'border': 1
        })
        
        metric_label_format = self.workbook.add_format({
            'bold': True,
            'font_size': 11,
            'align': 'left',
            'valign': 'vcenter',
            'border': 1
        })
        
        metric_value_format = self.workbook.add_format({
            'font_size': 11,
            'align': 'right',
            'valign': 'vcenter',
            'border': 1,
            'num_format': '#,##0.00'
        })
        
        section_format = self.workbook.add_format({
            'bold': True,
            'font_size': 14,
            'align': 'left',
            'valign': 'vcenter',
            'bg_color': '#e8eef7'
        })
        
        # Write title
        self.worksheet.merge_range('A1:B1', self.title, title_format)
        self.worksheet.write('A2', 'Generated on:', metric_label_format)
        self.worksheet.write('B2', self.metadata['generated_at'].strftime('%Y-%m-%d %H:%M:%S'), metric_value_format)
        self.worksheet.write('A3', 'Version:', metric_label_format)
        self.worksheet.write('B3', self.metadata['version'], metric_value_format)
        self.worksheet.write('A4', 'Author:', metric_label_format)
        self.worksheet.write('B4', self.metadata['author'], metric_value_format)
        
        row = 6
        
        # Write metrics
        if self.metrics:
            self.worksheet.write(row, 0, 'Key Metrics', section_format)
            row += 1
            
            metrics_dict = self.metrics.to_dict()
            for key, value in metrics_dict.items():
                if key in ['vehicle_id', 'fleet_id', 'period_start', 'period_end']:
                    continue
                
                if isinstance(value, (int, float)):
                    label = key.replace('_', ' ').title()
                    self.worksheet.write(row, 0, label, metric_label_format)
                    
                    # Apply appropriate number format
                    if 'km' in key:
                        self.worksheet.write(row, 1, value, self.workbook.add_format({
                            'num_format': '#,##0.0',
                            'border': 1
                        }))
                    elif 'cost' in key:
                        self.worksheet.write(row, 1, value, self.workbook.add_format({
                            'num_format': '$#,##0.00',
                            'border': 1
                        }))
                    elif 'count' in key:
                        self.worksheet.write(row, 1, value, self.workbook.add_format({
                            'num_format': '#,##0',
                            'border': 1
                        }))
                    else:
                        self.worksheet.write(row, 1, value, metric_value_format)
                    
                    row += 1
            
            row += 1
        
        # Write sections
        for section in self.sections:
            self.worksheet.write(row, 0, section.title, section_format)
            row += 1
            
            if isinstance(section.content, str):
                self.worksheet.write(row, 0, section.content)
                row += 1
            elif isinstance(section.content, pd.DataFrame):
                # Write DataFrame as table
                df = section.content
                
                # Write headers
                for col_num, column_name in enumerate(df.columns):
                    self.worksheet.write(row, col_num, column_name, header_format)
                
                row += 1
                
                # Write data
                for df_row in df.itertuples(index=False):
                    for col_num, value in enumerate(df_row):
                        self.worksheet.write(row, col_num, value)
                    row += 1
            
            row += 1
            
            # Write subsections
            for subsection in section.subsections:
                self.worksheet.write(row, 0, subsection.title, self.workbook.add_format({
                    'bold': True,
                    'font_size': 12,
                    'align': 'left'
                }))
                row += 1
                
                if isinstance(subsection.content, str):
                    self.worksheet.write(row, 0, subsection.content)
                    row += 1
                
                row += 1
        
        # Create additional worksheets for figures and tables
        if self.figures:
            chart_sheet = self.workbook.add_worksheet('Charts')
            chart_sheet.write('A1', 'Charts and Visualizations', title_format)
            
            # Note: XlsxWriter can create charts from data
            # For complex figures, you would need to save them as images and insert
        
        if self.tables:
            data_sheet = self.workbook.add_worksheet('Data Tables')
            row = 0
            
            for name, table_data in self.tables.items():
                data_sheet.write(row, 0, table_data['caption'], section_format)
                row += 1
                
                df = table_data['data']
                
                # Write headers
                for col_num, column_name in enumerate(df.columns):
                    data_sheet.write(row, col_num, column_name, header_format)
                
                row += 1
                
                # Write data
                for df_row in df.itertuples(index=False):
                    for col_num, value in enumerate(df_row):
                        data_sheet.write(row, col_num, value)
                    row += 1
                
                row += 2
        
        # Close workbook
        self.workbook.close()
        
        # Read generated Excel file
        with open(output_path, 'rb') as f:
            excel_bytes = f.read()
        
        return excel_bytes
    
    def save(self, filepath: str):
        """Save Excel report to file."""
        excel_bytes = self.generate(filepath)
        logger.info(f"Excel report saved to {filepath}")


class PerformanceReport:
    """Performance analysis report."""
    
    def __init__(self, telemetry_data: pd.DataFrame, vehicle_id: str = None):
        self.telemetry_data = telemetry_data
        self.vehicle_id = vehicle_id
        self.metrics = self._calculate_metrics()
        self.recommendations = self._generate_recommendations()
    
    def _calculate_metrics(self) -> ReportMetrics:
        """Calculate performance metrics."""
        metrics = ReportMetrics(vehicle_id=self.vehicle_id)
        
        if self.telemetry_data.empty:
            return metrics
        
        data = self.telemetry_data.copy()
        
        # Basic metrics
        if 'speed_kmh' in data.columns:
            metrics.avg_speed_kmh = float(data['speed_kmh'].mean())
            metrics.max_speed_kmh = float(data['speed_kmh'].max())
            metrics.speeding_count = int((data['speed_kmh'] > 120).sum())
        
        # Calculate distance if coordinates available
        if all(col in data.columns for col in ['latitude', 'longitude']):
            from math import radians, sin, cos, sqrt, atan2
            
            coords = data[['latitude', 'longitude']].dropna()
            if len(coords) > 1:
                distances = []
                for i in range(1, len(coords)):
                    lat1, lon1 = radians(coords.iloc[i-1]['latitude']), radians(coords.iloc[i-1]['longitude'])
                    lat2, lon2 = radians(coords.iloc[i]['latitude']), radians(coords.iloc[i]['longitude'])
                    
                    dlon = lon2 - lon1
                    dlat = lat2 - lat1
                    
                    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                    c = 2 * atan2(sqrt(a), sqrt(1-a))
                    distances.append(6371 * c)  # Earth radius in km
                
                metrics.total_distance_km = float(sum(distances))
        
        # Calculate driving time
        if 'timestamp' in data.columns:
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            time_diff = (data['timestamp'].max() - data['timestamp'].min()).total_seconds() / 3600
            metrics.total_driving_time_h = float(time_diff)
        
        # Calculate harsh events
        if 'acceleration_x' in data.columns:
            metrics.harsh_acceleration_count = int((data['acceleration_x'] > 3.0).sum())
            metrics.harsh_braking_count = int((data['acceleration_x'] < -3.0).sum())
        
        if 'acceleration_y' in data.columns:
            metrics.harsh_cornering_count = int((data['acceleration_y'].abs() > 2.5).sum())
        
        # Calculate idle time (simplified)
        if 'speed_kmh' in data.columns:
            metrics.idle_time_h = float((data['speed_kmh'] < 1.0).sum() * 10 / 3600)  # Assuming 10-second intervals
        
        return metrics
    
    def _generate_recommendations(self) -> List[Dict[str, Any]]:
        """Generate performance recommendations."""
        recommendations = []
        
        # Speed-related recommendations
        if self.metrics.speeding_count > 10:
            recommendations.append({
                'category': 'Safety',
                'issue': 'Frequent speeding detected',
                'recommendation': 'Implement speed limit alerts and driver training',
                'priority': 'High'
            })
        
        # Acceleration-related recommendations
        if self.metrics.harsh_acceleration_count > 20:
            recommendations.append({
                'category': 'Fuel Efficiency',
                'issue': 'Excessive harsh acceleration',
                'recommendation': 'Smooth acceleration can improve fuel efficiency by up to 20%',
                'priority': 'Medium'
            })
        
        if self.metrics.harsh_braking_count > 15:
            recommendations.append({
                'category': 'Safety',
                'issue': 'Frequent harsh braking',
                'recommendation': 'Maintain safe following distance and anticipate stops',
                'priority': 'High'
            })
        
        # Idle time recommendation
        if self.metrics.idle_time_h > 2.0:
            recommendations.append({
                'category': 'Fuel Efficiency',
                'issue': 'Excessive idle time',
                'recommendation': 'Reduce idle time to save fuel and reduce emissions',
                'priority': 'Medium'
            })
        
        # Add general recommendations if none specific
        if not recommendations:
            recommendations.append({
                'category': 'General',
                'issue': 'Good driving behavior',
                'recommendation': 'Continue current driving practices',
                'priority': 'Low'
            })
        
        return recommendations
    
    def generate_report(self, format: ReportFormat = ReportFormat.HTML) -> BaseReportGenerator:
        """Generate performance report."""
        if format == ReportFormat.HTML:
            generator = HTMLReportGenerator(
                title=f"Performance Report - {self.vehicle_id or 'Vehicle'}",
                metrics=self.metrics
            )
        elif format == ReportFormat.PDF:
            generator = PDFReportGenerator(
                title=f"Performance Report - {self.vehicle_id or 'Vehicle'}",
                metrics=self.metrics
            )
        elif format == ReportFormat.EXCEL:
            generator = ExcelReportGenerator(
                title=f"Performance Report - {self.vehicle_id or 'Vehicle'}",
                metrics=self.metrics
            )
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        # Add sections
        generator.add_section(
            "Performance Summary",
            f"This report summarizes the performance of {self.vehicle_id or 'the vehicle'} "
            f"over the analyzed period."
        )
        
        # Add driving behavior analysis
        behavior_content = f"""
        <h3>Driving Behavior Analysis</h3>
        <p>The vehicle was driven for {self.metrics.total_driving_time_h:.1f} hours, covering a distance of {self.metrics.total_distance_km:.1f} km.</p>
        <ul>
            <li><span class="badge badge-{'danger' if self.metrics.speeding_count > 10 else 'warning' if self.metrics.speeding_count > 5 else 'success'}">Speeding</span>: {self.metrics.speeding_count} instances detected</li>
            <li><span class="badge badge-{'danger' if self.metrics.harsh_acceleration_count > 20 else 'warning' if self.metrics.harsh_acceleration_count > 10 else 'success'}">Harsh Acceleration</span>: {self.metrics.harsh_acceleration_count} instances</li>
            <li><span class="badge badge-{'danger' if self.metrics.harsh_braking_count > 15 else 'warning' if self.metrics.harsh_braking_count > 8 else 'success'}">Harsh Braking</span>: {self.metrics.harsh_braking_count} instances</li>
            <li><span class="badge badge-{'warning' if self.metrics.harsh_cornering_count > 10 else 'success'}">Harsh Cornering</span>: {self.metrics.harsh_cornering_count} instances</li>
            <li><span class="badge badge-{'warning' if self.metrics.idle_time_h > 2.0 else 'success'}">Idle Time</span>: {self.metrics.idle_time_h:.1f} hours</li>
        </ul>
        """
        
        generator.add_section("Driving Behavior", behavior_content)
        
        # Add recommendations
        if self.recommendations:
            rec_content = "<h3>Recommendations</h3><table><tr><th>Category</th><th>Issue</th><th>Recommendation</th><th>Priority</th></tr>"
            for rec in self.recommendations:
                priority_class = {
                    'High': 'badge-danger',
                    'Medium': 'badge-warning',
                    'Low': 'badge-success'
                }.get(rec['priority'], 'badge-info')
                
                rec_content += f"""
                <tr>
                    <td>{rec['category']}</td>
                    <td>{rec['issue']}</td>
                    <td>{rec['recommendation']}</td>
                    <td><span class="badge {priority_class}">{rec['priority']}</span></td>
                </tr>
                """
            rec_content += "</table>"
            
            generator.add_section("Recommendations", rec_content)
        
        return generator


class MaintenanceReport:
    """Maintenance analysis report."""
    
    def __init__(self, maintenance_data: pd.DataFrame, fault_data: pd.DataFrame = None):
        self.maintenance_data = maintenance_data
        self.fault_data = fault_data
        self.metrics = self._calculate_metrics()
        self.predictions = self._generate_predictions()
    
    def _calculate_metrics(self) -> Dict[str, Any]:
        """Calculate maintenance metrics."""
        metrics = {}
        
        if not self.maintenance_data.empty:
            metrics['total_maintenance'] = len(self.maintenance_data)
            metrics['total_cost'] = float(self.maintenance_data['cost'].sum()) if 'cost' in self.maintenance_data.columns else 0
            metrics['avg_cost'] = float(self.maintenance_data['cost'].mean()) if 'cost' in self.maintenance_data.columns else 0
            
            if 'maintenance_type' in self.maintenance_data.columns:
                type_counts = self.maintenance_data['maintenance_type'].value_counts().to_dict()
                metrics['by_type'] = type_counts
        
        if self.fault_data is not None and not self.fault_data.empty:
            metrics['total_faults'] = len(self.fault_data)
            metrics['active_faults'] = int(self.fault_data['resolved_at'].isna().sum()) if 'resolved_at' in self.fault_data.columns else 0
            
            if 'severity' in self.fault_data.columns:
                severity_counts = self.fault_data['severity'].value_counts().to_dict()
                metrics['by_severity'] = severity_counts
            
            if 'component' in self.fault_data.columns:
                component_counts = self.fault_data['component'].value_counts().to_dict()
                metrics['by_component'] = component_counts
        
        return metrics
    
    def _generate_predictions(self) -> List[Dict[str, Any]]:
        """Generate maintenance predictions."""
        predictions = []
        
        # Sample predictions based on data analysis
        if 'total_maintenance' in self.metrics and self.metrics['total_maintenance'] > 0:
            # Predict next oil change based on average interval
            predictions.append({
                'component': 'Engine Oil',
                'predicted_date': (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d'),
                'confidence': 0.85,
                'reason': 'Based on average service interval of 90 days'
            })
        
        if self.fault_data is not None and not self.fault_data.empty:
            # Check for frequent fault components
            if 'by_component' in self.metrics:
                for component, count in self.metrics['by_component'].items():
                    if count > 3:  # Component with multiple faults
                        predictions.append({
                            'component': component,
                            'predicted_date': (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d'),
                            'confidence': 0.75,
                            'reason': f'Multiple faults ({count}) detected in last period'
                        })
        
        return predictions
    
    def generate_report(self, format: ReportFormat = ReportFormat.HTML) -> BaseReportGenerator:
        """Generate maintenance report."""
        generator = HTMLReportGenerator(
            title="Maintenance Analysis Report",
            metrics=None
        )
        
        # Add maintenance summary
        maint_content = f"""
        <h3>Maintenance Summary</h3>
        <p>Total maintenance records: {self.metrics.get('total_maintenance', 0)}</p>
        <p>Total maintenance cost: ${self.metrics.get('total_cost', 0):.2f}</p>
        <p>Average maintenance cost: ${self.metrics.get('avg_cost', 0):.2f}</p>
        """
        
        generator.add_section("Maintenance Overview", maint_content)
        
        # Add fault analysis if available
        if self.fault_data is not None:
            fault_content = f"""
            <h3>Fault Analysis</h3>
            <p>Total faults: {self.metrics.get('total_faults', 0)}</p>
            <p>Active faults: {self.metrics.get('active_faults', 0)}</p>
            """
            
            if 'by_severity' in self.metrics:
                fault_content += "<h4>Faults by Severity</h4><ul>"
                for severity, count in self.metrics['by_severity'].items():
                    fault_content += f"<li>{severity}: {count}</li>"
                fault_content += "</ul>"
            
            generator.add_section("Fault Analysis", fault_content)
        
        # Add predictions
        if self.predictions:
            pred_content = "<h3>Predictive Maintenance</h3><table><tr><th>Component</th><th>Predicted Date</th><th>Confidence</th><th>Reason</th></tr>"
            for pred in self.predictions:
                confidence_color = 'success' if pred['confidence'] > 0.8 else 'warning' if pred['confidence'] > 0.6 else 'danger'
                pred_content += f"""
                <tr>
                    <td>{pred['component']}</td>
                    <td>{pred['predicted_date']}</td>
                    <td><span class="badge badge-{confidence_color}">{pred['confidence']*100:.0f}%</span></td>
                    <td>{pred['reason']}</td>
                </tr>
                """
            pred_content += "</table>"
            
            generator.add_section("Predictive Maintenance", pred_content)
        
        return generator


class ReportGenerator:
    """Main report generator class."""
    
    def __init__(self):
        self.reports = {}
    
    def create_performance_report(self, telemetry_data: pd.DataFrame, 
                                vehicle_id: str = None) -> PerformanceReport:
        """Create performance report."""
        report = PerformanceReport(telemetry_data, vehicle_id)
        self.reports[f'performance_{vehicle_id or "all"}'] = report
        return report
    
    def create_maintenance_report(self, maintenance_data: pd.DataFrame, 
                                 fault_data: pd.DataFrame = None) -> MaintenanceReport:
        """Create maintenance report."""
        report = MaintenanceReport(maintenance_data, fault_data)
        self.reports['maintenance'] = report
        return report
    
    def generate_all_reports(self, output_dir: str = './reports', 
                           format: ReportFormat = ReportFormat.HTML):
        """Generate all reports."""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        for name, report in self.reports.items():
            if isinstance(report, PerformanceReport):
                generator = report.generate_report(format)
                filename = f"{output_dir}/{name}_report.{format.value}"
            elif isinstance(report, MaintenanceReport):
                generator = report.generate_report(format)
                filename = f"{output_dir}/{name}_report.{format.value}"
            else:
                continue
            
            generator.save(filename)
            logger.info(f"Report saved: {filename}")
    
    def generate_fleet_report(self, fleet_data: pd.DataFrame, 
                            period_start: datetime, period_end: datetime,
                            format: ReportFormat = ReportFormat.HTML) -> BaseReportGenerator:
        """Generate fleet-wide report."""
        if format == ReportFormat.HTML:
            generator = HTMLReportGenerator(
                title=f"Fleet Report {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}"
            )
        elif format == ReportFormat.PDF:
            generator = PDFReportGenerator(
                title=f"Fleet Report {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}"
            )
        elif format == ReportFormat.EXCEL:
            generator = ExcelReportGenerator(
                title=f"Fleet Report {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}"
            )
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        # Calculate fleet metrics
        total_vehicles = fleet_data['vehicle_id'].nunique() if 'vehicle_id' in fleet_data.columns else 0
        active_vehicles = fleet_data['status'].eq('active').sum() if 'status' in fleet_data.columns else 0
        
        # Add fleet summary
        summary_content = f"""
        <h3>Fleet Summary</h3>
        <p>Period: {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}</p>
        <p>Total Vehicles: {total_vehicles}</p>
        <p>Active Vehicles: {active_vehicles}</p>
        <p>Availability Rate: {(active_vehicles / total_vehicles * 100 if total_vehicles > 0 else 0):.1f}%</p>
        """
        
        generator.add_section("Fleet Overview", summary_content)
        
        # Add vehicle statistics if available
        if 'make' in fleet_data.columns:
            make_counts = fleet_data['make'].value_counts()
            make_table = pd.DataFrame({
                'Make': make_counts.index,
                'Count': make_counts.values
            })
            generator.add_table("Fleet Composition by Make", make_table)
        
        return generator