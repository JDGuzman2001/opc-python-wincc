# PLC Data Uploader to Google Cloud SQL

This Python application reads PLC (Programmable Logic Controller) data from a CSV file and uploads it to a PostgreSQL database in Google Cloud SQL.

## Description

The system is designed to monitor PLC variables from a chocolate production line, including:
- **Digital Inputs (DI16xDC24V)**: Conveyor belt sensors, emergency buttons, jar indicators
- **Digital Outputs (DO8xDC24V_2A)**: Conveyor belt motors, rotator activation
- **Analog Inputs (AI8x13Bit)**: Repetition counters

## Prerequisites

Before running this application, ensure you have installed:

- Python 3.x
- Google Cloud credentials configured
- PostgreSQL database in Google Cloud SQL

## Installation

1. Install Python dependencies:

```bash
pip install -r requirements.txt
```

2. Configure environment variables in a `.env` file:

```env
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account-key.json
PROJECT_ID=your-google-cloud-project-id
SQL_DATABASE_NAME=your-database-name
SQL_DATABASE_USERNAME=your-database-username
SQL_DATABASE_PASSWORD=your-database-password
SQL_DATABASE_REGION=your-database-region
SQL_DATABASE_INSTANCE=your-database-instance
OPENAI_API_KEY=your-openai-api-key
```

## Usage

1. Make sure your `EXPORT.csv` file contains the PLC variable data in the correct format
2. Run the application:

```bash
python main.py
```

## CSV File Format

The application expects a CSV file with the following structure:
- **Headers**: Variable names (e.g., "MotorVerdesIn", "MotorRojosIn", "Rotador", etc.)
- **Data rows**: Each row contains timestamps and corresponding values for each variable
- **Format**: Semicolon-separated values with quoted fields
- **Example**:
  ```
  "MotorVerdesIn";"Valor1";"MotorRojosIn";"Valor2";"Rotador";"Valor4"
  26/06/2025 15:01:52;0;26/06/2025 15:01:52;0;26/06/2025 15:01:52;0
  ```

## Module Assignment

The application automatically assigns variables to their corresponding PLC modules based on symbol names:

### Digital Inputs (DI16xDC24V)
- Start, Emergencia, BandaVI, BandaRI, BandaMI
- TarrosListos, TarrosVM, TarrosRM, OnRotator

### Digital Outputs (DO8xDC24V_2A)
- Rotador, FinRojoVerde, MotorVI, MotorRI, MotorMI
- MotorVO, MotorRO, MotorMO
- **CSV Variables**: MotorVerdesIn, MotorRojosIn, MotorMoradosIn, MotorMoradosOut

### Analog Inputs (AI8x13Bit)
- Repeticiones

## Functionality

The application performs the following operations:

1. **Data Reading**: Reads the `EXPORT.csv` file containing PLC variables with timestamps
2. **Module Assignment**: Automatically assigns variables to their corresponding PLC modules based on symbol names
3. **Address Mapping**: Maps symbols to their PLC addresses (I 0.0, Q 0.0, MW 512, etc.)
4. **Table Creation**: Automatically creates the `chocolatin_variables_history` table if it doesn't exist
5. **Data Insertion**: Uploads all variable data to the database with:
   - Module (DI16xDC24V, DO8xDC24V_2A, AI8x13Bit)
   - PLC Address (I 0.0, Q 0.0, MW 512, etc.)
   - Symbol (variable name from CSV headers)
   - Data Type (BOOL for 0/1 values, WORD for others)
   - Descriptive comment (empty for CSV data)
   - Current value
   - Reading timestamp

## Database Structure

The `chocolatin_variables_history` table contains the following fields:

- `id`: Unique identifier (SERIAL PRIMARY KEY)
- `module`: PLC module (VARCHAR)
- `address`: PLC address (VARCHAR)
- `symbol`: Symbol name (VARCHAR)
- `data_type`: Data type (VARCHAR)
- `comment`: Descriptive comment (TEXT)
- `value`: Current value (TEXT)
- `timestamp`: Reading timestamp (TIMESTAMPTZ)
- `created_at`: Record creation date (TIMESTAMPTZ)

## Error Handling

The application includes robust error handling that:

- Verifies the existence of the CSV file
- Validates the CSV structure
- Handles database connection errors
- Performs rollback in case of insertion errors
- Properly closes database connections

## Notes

- The application is configured to work with specific variables from a chocolate production line
- Data is stored with timestamps for historical tracking
- Uses Google Cloud SQL Connector for secure database connections
- The `EXPORT.csv` file must follow the specified structure with variable names in headers and corresponding values in data rows
- Each row in the CSV represents a time point with values for all monitored variables
- Variables are automatically categorized into their appropriate PLC modules based on symbol names
