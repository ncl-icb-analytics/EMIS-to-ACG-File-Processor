# EMIS to ACG File Processor

This application provides a graphical user interface (GUI) to process clinical data extracts (typically from EMIS Web) and transform them into the file formats required by the Johns Hopkins ACG® System. It uses a mapping configuration file (`mapping.csv`) to determine how input data columns are transformed into the required ACG output columns.

**Disclaimer:** This repository and the associated application are not affiliated with, endorsed by, or connected to The Johns Hopkins University or the Johns Hopkins ACG® System in any way. ACG® is a registered trademark of The Johns Hopkins University.

## Features

* GUI for easy file selection and processing initiation.
* Drag-and-drop support for adding input CSV files.
* Automatic identification of input files based on column headers defined in `config.py`.
* Flexible data mapping and transformation logic driven by `mapping.csv`.
* Generation of ACG files:
  * Patient Data
  * Medical Services
  * Pharmacy Data
* Configurable output directory (defaults to Downloads).
* Processing log display within the application.
* Packaged as a standalone executable for easy distribution (using PyInstaller).
* Built-in help describing required input files based on `mapping.csv`.

## Requirements

* Python (Version 3.8+ recommended)
* Required Python packages listed in `requirements.txt`:
  * `pandas`
  * `PySide6`
  * `pyinstaller` (only needed for building the executable)

## Setup (for Development)

1. **Clone the Repository (if applicable):**
   ```bash
   git clone https://github.com/ncl-icb-analytics/EMIS-to-ACG-File-Processor
   cd file_combiner
   ```
2. **Create a Virtual Environment:**
   ```bash
   python -m venv venv
   ```
3. **Activate the Virtual Environment:**
   * Windows (PowerShell): `.\venv\Scripts\Activate.ps1`
   * Windows (CMD): `.\venv\Scripts\activate.bat`
   * macOS/Linux: `source venv/bin/activate`
4. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Configuration is managed through two main files:

1. **`config.py`:**

   * `MERGE_KEY`: Defines the primary patient identifier column used for linking across input files (e.g., `"PatientID"`). This column **must** exist in your input files.
   * `INPUT_FILE_COLUMNS`: A dictionary defining the *exact* set of expected column headers for each input CSV file type recognised by the application. The keys (e.g., `"Patient_Details"`, `"Care_History"`) are used internally and in `mapping.csv`. The application identifies dropped/browsed files by comparing their headers to these definitions.
   * `TRANSFORMATIONS`: A dictionary mapping transformation function names (used in `mapping.csv`) to the actual Python functions defined in this file (e.g., `format_date_yyyy_mm_dd`, `transform_sex`). You can add custom transformation functions here.
   * `OUTPUT_FILENAME_TEMPLATES`: Defines the naming pattern for the generated ACG output files.
2. **`mapping.csv`:**

   * This is the core file defining how data flows from input files to ACG output files.
   * **Required Columns:**
     * `InputConfigKey`: Matches a key from `INPUT_FILE_COLUMNS` in `config.py` (e.g., `Patient_Details`).
     * `InputColumn`: The specific column name from the source CSV file. Leave blank if the target column is generated solely by a `TransformationFunction`.
     * `TargetACGFile`: The target ACG file (`patient_data`, `medical_services`, or `pharmacy_data`).
     * `TargetACGColumn`: The specific column name required in the target ACG file.
   * **Optional Columns:**
     * `TransformationFunction`: The name of a function defined in `config.py`'s `TRANSFORMATIONS` dictionary to apply to the `InputColumn` data. If blank, data is mapped directly.
     * `SourceLabel`: Used for `medical_services` and `pharmacy_data` where multiple input files might contribute rows. Rows with the same `SourceLabel` are processed together from the specified `InputConfigKey`.

## Usage

### Running from Source

1. Ensure your virtual environment is activated.
2. Run the main application file:
   ```bash
   python main.py
   ```

### Using the Executable (`.exe`)

1. Navigate to the `dist` folder (created after building).
2. Double-click `EMIS_ACG_Processor.exe`.

### Application Interface

1. **Add Input Files (.csv):**
   * Drag and drop the required CSV files onto the designated area.
   * Alternatively, use the "Browse for File..." button.
   * Files are identified based on their *column headers* matching the definitions in `config.py`.
   * The status list shows which file types are needed or have been added.
   * Click "Required File Definitions" for help on expected columns and sample data based on `mapping.csv`.
2. **Output Destination:**
   * Displays the folder where generated ACG files will be saved (defaults to your Downloads folder).
   * Click "Change..." to select a different output folder.
3. **Processing Log:**
   * Shows status messages, progress, and any errors during file loading and processing.
4. **Action Buttons:**
   * **Process Files:** Becomes active when all required input files are added and a valid output directory is selected. Starts the ACG file generation process. Disabled after a successful run until inputs/output are changed.
   * **Open Output Folder:** Becomes active after files are successfully processed. Opens the selected output folder in your system's file explorer.

## Building the Executable

1. Ensure PyInstaller is installed (`pip install pyinstaller`).
2. Run PyInstaller using the spec file:
   ```bash
   pyinstaller main.spec
   ```
3. The standalone executable (`EMIS_ACG_Processor.exe`) will be located in the `dist` folder.

### Customising the Build

* **`main.spec`:** Modify this file for advanced PyInstaller options (e.g., adding other data files, specifying hidden imports if needed).

## Troubleshooting

* **`mapping.csv` Not Found (when running `.exe`):** Ensure the `datas=[('mapping.csv', '.')]` line is present in the `Analysis` block of `main.spec` and rebuild the executable. The code in `main.py` uses `sys._MEIPASS` to find the file when packaged.
* **`ImportError: No module named ...` (when running `.exe`):** PyInstaller might have missed a hidden dependency. Try adding the missing module name to the `hiddenimports` list in `main.spec` and rebuild. Common ones for pandas include `'pandas._libs.tslibs'`.
* **File Identification Errors:** Double-check that the column headers in your input CSV files *exactly* match the column lists defined in `config.py`'s `INPUT_FILE_COLUMNS` dictionary for the corresponding file type.
* **Processing Errors:** Check the Processing Log in the application for specific error messages related to data loading, mapping, or transformations. Verify `mapping.csv` and `config.py` are correctly configured.

## License

This repository is dual licensed under the Open Government Licence v3.0 and the MIT License.

Unless stated otherwise, all code outputs are subject to © Crown copyright.
