import sys
import os
import logging
import subprocess
import time
from datetime import datetime
import pandas as pd
from collections import defaultdict
import functools # Import functools

# Attempt to import necessary modules, provide guidance if missing
try:
    from PySide6.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QPushButton, QLabel, QLineEdit, QTextEdit, QFileDialog, QMessageBox,
        QSizePolicy, QFrame, QScrollArea, QGroupBox
    )
    from PySide6.QtCore import QThread, Signal, Slot, Qt, QObject, QMimeData, QUrl
    # Remove QIcon, QStyle imports
    from PySide6.QtGui import QPalette, QColor, QTextCursor, QDragEnterEvent, QDropEvent #, QIcon, QStyle
except ImportError:
    print("ERROR: PySide6 is not installed. Please install it using: pip install PySide6")
    sys.exit(1)

# Attempt to import local modules
try:
    import processing
    import config
except ImportError as e:
    print(f"ERROR: Failed to import local module ({e}). Make sure config.py and processing.py are in the same directory.")
    sys.exit(1)

# --- Constants ---
APP_NAME = "EMIS to ACG File Processor"
WINDOW_WIDTH = 800
WINDOW_HEIGHT = 800 # Increased height further

# --- Basic Styling (QSS) ---
STYLESHEET = """
    QMainWindow {
        background-color: #f8f9fa;
    }
    QGroupBox {
        font-size: 11pt;
        font-weight: bold;
        color: #343a40;
        border: 1px solid #dee2e6;
        border-radius: 5px;
        margin-top: 10px; /* Space above the group box */
        padding-top: 20px; /* Space for the title */
        padding-left: 10px;
        padding-right: 10px;
        padding-bottom: 10px;
    }
    QGroupBox::title {
        subcontrol-origin: margin;
        subcontrol-position: top left;
        left: 10px;
        padding-left: 5px;
        padding-right: 5px;
        background-color: #f8f9fa; /* Match window bg */
    }
    QLabel {
        font-size: 10pt;
        margin-bottom: 2px;
        color: #495057;
    }
    QLineEdit, QTextEdit {
        border: 1px solid #ced4da;
        border-radius: 4px;
        padding: 6px 8px;
        background-color: #ffffff;
        font-size: 10pt;
        color: #212529;
    }
    QLineEdit:focus, QTextEdit:focus {
        border-color: #80bdff;
    }
    QLineEdit:disabled, QLineEdit:read-only {
        background-color: #e9ecef;
        color: #6c757d;
        border-color: #ced4da;
    }
    QTextEdit:read-only {
        background-color: #e9ecef;
        color: #343a40;
        border-color: #ced4da;
    }
    QPushButton {
        background-color: #007bff;
        color: white;
        border: none;
        border-radius: 4px;
        padding: 8px 15px;
        font-size: 10pt;
        min-height: 22px;
        outline: none;
    }
    QPushButton:hover {
        background-color: #0056b3;
    }
    QPushButton:pressed {
        background-color: #004085;
    }
    QPushButton:disabled {
        background-color: #adb5bd;
        color: #f8f9fa;
    }
    #LogText {
        font-family: Consolas, 'Courier New', monospace;
        font-size: 9pt;
        color: #343a40;
    }
    #SubSectionLabel { /* Used for labels within group boxes */
        font-size: 10pt;
        font-weight: normal; /* Normal weight for sub-labels */
        margin-top: 8px;
        margin-bottom: 4px;
        color: #495057;
    }
    QFrame#SeparatorLine { /* No longer used with QGroupBox */
       /* border: none; */
       /* height: 0px; */
       /* margin: 0px; */
       /* padding: 0px; */
    }
    #DropAreaFrame {
        border: 2px dashed #adb5bd;
        border-radius: 5px;
        background-color: #f1f3f5;
        min-height: 60px;
        padding: 5px;
        margin-bottom: 10px;
    }
    #DropAreaFrame[dragOver="true"] {
        background-color: #cfe2ff;
        border-color: #0d6efd;
    }
    #DropAreaLabel {
        font-size: 10pt;
        color: #6c757d;
        background-color: transparent;
        border: none;
    }
    #FileStatusArea {
        /* Style the QScrollArea itself */
        border: 1px solid #ced4da;
        border-radius: 4px;
        background-color: #e9ecef;
        min-height: 75px; /* Reduced from 100px */
    }
    /* Remove potential default border/bg of the inner widget */
    #FileStatusArea > QWidget > QWidget {
        background-color: transparent;
        border: none;
    }
    #FileStatusLabel {
        font-size: 9pt;
        padding: 5px;
        color: #343a40;
        background-color: transparent; /* Ensure transparent background */
    }
    #StrategyText {
        font-family: Consolas, 'Courier New', monospace;
        font-size: 9pt;
        color: #343a40;
        border: 1px solid #ced4da;
        border-radius: 4px;
        padding: 5px;
        background-color: #e9ecef;
    }
    /* --- Primary Button Override (Explicit) --- */
    QPushButton#ProcessButton {
        background-color: #007bff;
    }
    QPushButton#ProcessButton:hover {
        background-color: #0056b3;
    }
    QPushButton#ProcessButton:pressed {
        background-color: #004085;
    }

    /* --- Primary Button Disabled Style --- */
    QPushButton#ProcessButton:disabled {
        background-color: #a0cfff; /* Lighter, desaturated blue */
        color: #6c757d; /* Medium grey text */
    }

    /* --- Secondary Button Style --- */
    QPushButton#BrowseButton,
    QPushButton#HelpButton,
    QPushButton#ChangeOutputButton,
    QPushButton#OpenFolderButton {
        background-color: #6c757d; /* Dark grey */
    }
    QPushButton#BrowseButton:hover,
    QPushButton#HelpButton:hover,
    QPushButton#ChangeOutputButton:hover,
    QPushButton#OpenFolderButton:hover {
        background-color: #5a6268; /* Darker grey */
    }
    QPushButton#BrowseButton:pressed,
    QPushButton#HelpButton:pressed,
    QPushButton#ChangeOutputButton:pressed,
    QPushButton#OpenFolderButton:pressed {
        background-color: #495057; /* Even darker grey */
    }
    /* Disabled state is handled by the general QPushButton:disabled rule */

    /* --- Remove Button Style --- */
    QPushButton#RemoveButton {
        font-family: "Consolas", monospace;
        font-weight: bold;
        max-width: 20px; /* Small button */
        min-height: 18px;
        padding: 1px 1px;
        background-color: #adb5bd; /* Grey */
        color: #ffffff;
        border-radius: 3px;
    }
    QPushButton#RemoveButton:hover {
        background-color: #dc3545; /* Red */
    }
    QPushButton#RemoveButton:pressed {
        background-color: #c82333; /* Darker Red */
    }
"""

# --- Logging Handler ---
class QtLogHandler(logging.Handler, QObject):
    new_log_record = Signal(str)
    def __init__(self):
        super().__init__()
        QObject.__init__(self)
    def emit(self, record):
        msg = self.format(record)
        self.new_log_record.emit(msg)

# --- Worker Thread ---
class ProcessingWorker(QThread):
    finished = Signal(bool, str, str)
    progress = Signal(str)
    def __init__(self, input_files_dict, output_dir):
        super().__init__()
        self.input_files_dict = input_files_dict # config_key: filepath
        self.output_dir = output_dir
    def run(self):
        success = False
        error_message = ""
        loaded_data_dict = {} # config_key: DataFrame
        try:
            self.progress.emit("--- Starting File Processing ---")

            # Load input files into DataFrames
            for config_key, file_path in self.input_files_dict.items():
                self.progress.emit(f"Loading file: {os.path.basename(file_path)} (as {config_key})...")
                try:
                    # Read as string to prevent pandas type guessing (important for IDs, codes)
                    df = pd.read_csv(file_path, dtype=str, encoding='utf-8')
                    # Basic validation: Check if merge key exists if it's not Patient_Details
                    # (Patient_Details check happens within processing._generate_patient_data)
                    if config_key != "Patient_Details" and config.MERGE_KEY not in df.columns:
                         raise ValueError(f"Merge key '{config.MERGE_KEY}' not found in columns of {config_key} file.")
                    loaded_data_dict[config_key] = df
                    self.progress.emit(f"Loaded {os.path.basename(file_path)} successfully ({len(df)} rows).")
                except FileNotFoundError:
                    raise RuntimeError(f"File not found: {file_path}")
                except pd.errors.EmptyDataError:
                    self.progress.emit(f"WARNING: File {os.path.basename(file_path)} is empty. Processing will continue, but this might cause issues.")
                    loaded_data_dict[config_key] = pd.DataFrame() # Assign empty DF
                except ValueError as ve:
                    raise RuntimeError(f"Data validation error in {os.path.basename(file_path)}: {ve}")
                except Exception as load_err:
                    raise RuntimeError(f"Error loading {os.path.basename(file_path)}: {load_err}")

            # Check if all expected files were loaded (even if empty)
            if len(loaded_data_dict) != len(self.input_files_dict):
                raise RuntimeError("Mismatch between expected files and loaded files. Check loading logs.")

            self.progress.emit("All input files loaded. Starting ACG generation...")
            # *** Pass the dictionary of DataFrames to the processing function ***
            processing.generate_acg_files(loaded_data_dict, self.output_dir)
            success = True
            self.progress.emit("--- File Processing Completed Successfully ---")

        except RuntimeError as rte:
            # Specific error expected from loading or processing steps
            logging.getLogger().error(f"Processing aborted due to runtime error: {rte}")
            error_message = str(rte)
            self.progress.emit(f"ERROR: {error_message}")
        except Exception as e:
            # Catch any other unexpected errors
            logging.getLogger().exception("An unexpected error occurred during processing.")
            error_message = str(e)
            self.progress.emit(f"ERROR during processing: {error_message}")
        finally:
            self.finished.emit(success, error_message, self.output_dir if success else None)

# --- Main Window ---
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.added_files = {}
        self.last_successful_output_dir = None
        self.expected_file_count = 0
        self.all_config_keys = set()
        self.config_column_sets = {}
        self.merge_key = ""
        self.worker = None
        self.output_dir = self._get_default_output_dir() # Initialize default output dir

        # Load config and check critical values
        try:
            self.all_config_keys = set(config.INPUT_FILE_COLUMNS.keys())
            self.expected_file_count = len(self.all_config_keys)
            self.config_column_sets = {key: set(cols) for key, cols in config.INPUT_FILE_COLUMNS.items()}
            self.merge_key = config.MERGE_KEY
            if not self.merge_key:
                raise AttributeError("MERGE_KEY is empty in config.py")
        except AttributeError as e:
             QMessageBox.critical(self, "Config Error", f"Configuration error in config.py: {e}. Please check the file.")
             self.expected_file_count = -1
             self.all_config_keys = set()
             self.config_column_sets = {}
             self.merge_key = ""

        self.setWindowTitle(APP_NAME)
        self.setGeometry(100, 100, WINDOW_WIDTH, WINDOW_HEIGHT)
        self.setStyleSheet(STYLESHEET)
        self.setAcceptDrops(True)

        self.init_ui()
        self.setup_logging()
        self._update_file_status_display() # Initial status display
        self.update_ui_state()

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(10, 10, 10, 10) # Reduced main margins slightly
        main_layout.setSpacing(15) # Spacing between main sections

        # --- GroupBox 1: Input Files --- #
        input_group_box = QGroupBox("1. Add Input Files (.csv)")
        input_section_layout = QVBoxLayout(input_group_box)
        input_section_layout.setSpacing(10)

        # Drop Area
        self.drop_area_frame = QFrame()
        self.drop_area_frame.setObjectName("DropAreaFrame")
        self.drop_area_frame.setProperty("dragOver", False)
        self.drop_area_frame.setAcceptDrops(True)
        drop_area_layout = QVBoxLayout(self.drop_area_frame)
        drop_area_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.drop_area_label = QLabel(f"Drop required CSV files here (one or more at a time)")
        self.drop_area_label.setObjectName("DropAreaLabel")
        self.drop_area_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        drop_area_layout.addWidget(self.drop_area_label)
        input_section_layout.addWidget(self.drop_area_frame)

        # Browse Button and Help Button Row
        browse_help_hbox = QHBoxLayout()
        self.browse_file_button = QPushButton("Browse for File...")
        self.browse_file_button.setObjectName("BrowseButton")
        self.browse_file_button.clicked.connect(self.browse_for_file)
        browse_help_hbox.addWidget(self.browse_file_button)
        browse_help_hbox.addStretch(1) # Push help button to the right

        self.help_button = QPushButton("Required File Definitions") # Changed text
        self.help_button.setObjectName("HelpButton")
        # Remove icon lines completely
        # icon = QApplication.style().standardIcon(QStyle.StandardPixmap.SP_MessageBoxInformation)
        # self.help_button.setIcon(icon)
        self.help_button.setToolTip("Show help about required input files based on mapping.csv")
        # Restore connection
        self.help_button.clicked.connect(self.show_input_file_help)
        browse_help_hbox.addWidget(self.help_button)
        input_section_layout.addLayout(browse_help_hbox)

        # --- Status Label and Helper Text (Vertical Layout) ---
        # Remove QHBoxLayout
        status_label = QLabel("Input File Status:")
        status_label.setObjectName("SubSectionLabel")
        # Remove explicit VCenter alignment - let layout manage
        input_section_layout.addWidget(status_label) # Add status label directly

        self.file_status_area = QScrollArea()
        self.file_status_area.setObjectName("FileStatusArea")
        self.file_status_area.setWidgetResizable(True)
        self.file_status_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff) # Disable vertical scrollbar
        self.file_status_content = QWidget() # Use a QWidget for the scroll area content
        self.file_status_layout = QVBoxLayout(self.file_status_content)
        self.file_status_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.file_status_layout.setContentsMargins(5, 5, 5, 5) # Padding inside status area
        self.file_status_area.setWidget(self.file_status_content)
        input_section_layout.addWidget(self.file_status_area, 0) # Set stretch factor to 0

        # Add helper text *below* the status area
        content_info_label = QLabel("(Files are identified by their columns, not filename)")
        content_info_label.setStyleSheet("font-size: 8pt; color: #6c757d; padding-top: 3px;") # Add a little top padding
        input_section_layout.addWidget(content_info_label)

        main_layout.addWidget(input_group_box)

        # --- GroupBox 2: Output Destination --- #
        output_group_box = QGroupBox("2. Output Destination")
        output_layout = QHBoxLayout(output_group_box)
        output_layout.setSpacing(10)
        output_label = QLabel("Output Folder:")
        self.output_dir_display = QLineEdit(self.output_dir) # Display initial default
        self.output_dir_display.setReadOnly(True)
        self.change_output_button = QPushButton("Change...")
        self.change_output_button.setObjectName("ChangeOutputButton")
        self.change_output_button.clicked.connect(self.select_output_directory)
        output_layout.addWidget(output_label)
        output_layout.addWidget(self.output_dir_display, 1) # Allow display to stretch
        output_layout.addWidget(self.change_output_button)
        main_layout.addWidget(output_group_box)

        # --- GroupBox 3: Log --- #
        log_group_box = QGroupBox("3. Processing Log") # Renumbered title
        log_layout = QVBoxLayout(log_group_box)
        self.log_text_edit = QTextEdit()
        self.log_text_edit.setObjectName("LogText")
        self.log_text_edit.setReadOnly(True)
        log_layout.addWidget(self.log_text_edit)
        main_layout.addWidget(log_group_box, 1) # Allow log groupbox to stretch

        # --- Action Buttons --- #
        button_hbox = QHBoxLayout()
        button_hbox.setSpacing(10)
        self.process_button = QPushButton("Process Files")
        self.process_button.setObjectName("ProcessButton")
        self.process_button.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.process_button.clicked.connect(self.start_processing)
        button_hbox.addWidget(self.process_button)
        self.open_folder_button = QPushButton("Open Output Folder")
        self.open_folder_button.setObjectName("OpenFolderButton")
        self.open_folder_button.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.open_folder_button.clicked.connect(self.open_output_folder)
        button_hbox.addWidget(self.open_folder_button)
        main_layout.addLayout(button_hbox)


    def setup_logging(self):
        self.log_handler = QtLogHandler()
        log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
        self.log_handler.setFormatter(log_formatter)
        root_logger = logging.getLogger()
        processing_logger = logging.getLogger(processing.__name__)
        root_logger.handlers.clear()
        processing_logger.handlers.clear()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(self.log_handler)
        processing_logger.setLevel(logging.INFO)
        processing_logger.addHandler(self.log_handler)
        processing_logger.propagate = False
        self.log_handler.new_log_record.connect(self.update_log)
        if self.expected_file_count >= 0:
            logging.info(f"Application ready. Drop or browse for {self.expected_file_count} required input files.")

    def _identify_file_type(self, filepath):
        """Identifies the configuration key based on file columns (case-insensitive)."""
        if not self.config_column_sets: return None, "Config error"
        try:
            df_header = pd.read_csv(filepath, nrows=0, dtype=str)
            # Convert actual file columns to lowercase set
            file_columns_lower = {col.lower() for col in df_header.columns}
            possible_matches = []
            for key, config_cols in self.config_column_sets.items():
                # Convert configured columns to lowercase set for comparison
                config_cols_lower = {col.lower() for col in config_cols}
                if file_columns_lower == config_cols_lower:
                    possible_matches.append(key)
            if len(possible_matches) == 1:
                return possible_matches[0], None
            elif len(possible_matches) > 1:
                return None, f"Matches multiple types: {possible_matches}"
            else:
                error_detail = [f"{col} not found in {key}" for col in file_columns_lower if col not in config_cols_lower]
                return None, f"Columns do not match any required type ({'; '.join(error_detail)})"
        except FileNotFoundError:
            return None, "File not found"
        except pd.errors.EmptyDataError:
            return None, "File is empty"
        except Exception as e:
            logging.error(f"Error reading header from {filepath}: {e}")
            return None, f"Error reading header (check format/permissions)"

    def _clear_layout(self, layout):
        """Helper function to remove all widgets from a layout."""
        if layout is not None:
            while layout.count():
                item = layout.takeAt(0)
                widget = item.widget()
                if widget is not None:
                    widget.deleteLater()
                else:
                    child_layout = item.layout()
                    if child_layout is not None:
                        self._clear_layout(child_layout)
                        # Optionally delete the layout itself if it's dynamically created
                        # child_layout.deleteLater() # Be cautious with this

    def _update_file_status_display(self):
        """Dynamically builds the file status display with remove buttons."""
        self._clear_layout(self.file_status_layout) # Clear previous widgets

        if self.expected_file_count < 0: # Config error
            error_label = QLabel("<font color='red'>Configuration Error!</font>")
            self.file_status_layout.addWidget(error_label)
            return

        if not self.all_config_keys:
             no_keys_label = QLabel("No input files defined in config.")
             self.file_status_layout.addWidget(no_keys_label)
             return

        # Dynamically create rows for each file type
        for key in sorted(list(self.all_config_keys)):
            row_layout = QHBoxLayout()
            row_layout.setSpacing(10)

            type_label = QLabel(f"<b>{key.replace('_',' ')}:</b>")
            type_label.setFixedWidth(150) # Fixed width for alignment
            row_layout.addWidget(type_label)

            if key in self.added_files:
                filename = os.path.basename(self.added_files[key])
                status_label = QLabel(f"<font color='green'>Added:</font> {filename}")
                status_label.setToolTip(self.added_files[key]) # Show full path on hover
                row_layout.addWidget(status_label, 1) # Allow status label to stretch

                remove_button = QPushButton("X")
                remove_button.setObjectName("RemoveButton")
                remove_button.setToolTip(f"Remove {filename}")
                # Use partial to pass the key to the remove function
                remove_button.clicked.connect(functools.partial(self.remove_input_file, key))
                row_layout.addWidget(remove_button)
            else:
                status_label = QLabel("<font color='#F57C00'>Needed</font>")
                row_layout.addWidget(status_label, 1) # Allow status label to stretch
                # Add a spacer to keep alignment consistent with rows that have a button
                spacer = QWidget()
                spacer.setFixedWidth(22) # Approximate width of remove button + spacing
                row_layout.addWidget(spacer)

            self.file_status_layout.addLayout(row_layout)

        # Add stretch at the end to push rows to the top
        self.file_status_layout.addStretch(1)

    def _process_file_addition(self, path):
        """Handles the logic for adding a single file (from drop or browse)."""
        file_key, error = self._identify_file_type(path)
        if file_key:
            if file_key in self.added_files:
                reply = QMessageBox.question(self, "Replace File?",
                                           f"A file for type '{file_key}' ({os.path.basename(self.added_files[file_key])}) has already been added.\nDo you want to replace it with {os.path.basename(path)}?",
                                           QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                           QMessageBox.StandardButton.No)
                if reply == QMessageBox.StandardButton.Yes:
                    self.added_files[file_key] = path
                    logging.info(f"Replaced file for type '{file_key}' with: {path}")
                    return True
                else:
                    logging.info(f"Kept existing file for type '{file_key}'. Ignored: {path}")
                    return False
            else:
                self.added_files[file_key] = path
                logging.info(f"Added file for type '{file_key}': {path}")
                return True
        else:
            logging.warning(f"Could not identify file '{os.path.basename(path)}': {error}")
            QMessageBox.warning(self, "File Identification Error",
                                f"Could not identify file: {os.path.basename(path)}\nReason: {error}")
            return False

    @Slot(str) # Add Slot decorator and type hint
    def remove_input_file(self, file_key_to_remove):
        """Removes a file from the added list and updates the UI."""
        if file_key_to_remove in self.added_files:
            removed_path = self.added_files.pop(file_key_to_remove)
            logging.info(f"Removed file for type '{file_key_to_remove}': {os.path.basename(removed_path)}")
            self.last_successful_output_dir = None # Reset success state
            self._update_file_status_display() # Refresh the list
            self.update_ui_state() # Update button enable states
        else:
            logging.warning(f"Attempted to remove file key '{file_key_to_remove}' which was not found in added files.")

    # --- Drag and Drop Events --- #
    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
            self.drop_area_frame.setProperty("dragOver", True)
            self._refresh_style(self.drop_area_frame)
        else:
            event.ignore()
            self.drop_area_frame.setProperty("dragOver", False)
            self._refresh_style(self.drop_area_frame)

    def dragMoveEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dragLeaveEvent(self, event):
        self.drop_area_frame.setProperty("dragOver", False)
        self._refresh_style(self.drop_area_frame)
        event.accept()

    def dropEvent(self, event: QDropEvent):
        self.drop_area_frame.setProperty("dragOver", False)
        self._refresh_style(self.drop_area_frame)
        if self.expected_file_count < 0: event.ignore(); return
        if not event.mimeData().hasUrls(): event.ignore(); return
        event.setDropAction(Qt.DropAction.CopyAction)
        event.accept()
        files_changed = False
        for url in event.mimeData().urls():
            if url.isLocalFile():
                path = url.toLocalFile()
                if os.path.isfile(path) and path.lower().endswith('.csv'):
                    if self._process_file_addition(path):
                        files_changed = True
                else:
                    logging.warning(f"Ignoring non-CSV file or directory dropped: {path}")
            else:
                 logging.warning(f"Ignoring non-local URL dropped: {url.toString()}")
        if files_changed:
            self._update_file_status_display()
            self.last_successful_output_dir = None
            self.update_ui_state()

    def _refresh_style(self, widget):
        widget.style().unpolish(widget)
        widget.style().polish(widget)
    # --- End Drag and Drop --- #

    @Slot()
    def browse_for_file(self):
        """Slot for the 'Browse for File...' button."""
        if self.expected_file_count < 0: return
        filepath, _ = QFileDialog.getOpenFileName(self, f"Select an Input File", "", "CSV Files (*.csv);;All Files (*)")
        if filepath:
            if self._process_file_addition(filepath):
                self._update_file_status_display()
                self.last_successful_output_dir = None
                self.update_ui_state()

    @Slot(str)
    def update_log(self, message):
        self.log_text_edit.moveCursor(QTextCursor.MoveOperation.End)
        self.log_text_edit.insertPlainText(message + '\n')
        self.log_text_edit.moveCursor(QTextCursor.MoveOperation.End)

    def update_ui_state(self):
        process_enabled = (
            len(self.added_files) == self.expected_file_count and
            self.expected_file_count >= 0 and
            bool(self.output_dir) and # Check if output dir is set
            self.last_successful_output_dir is None and # Check if not already succeeded
            (self.worker is None or not self.worker.isRunning())
        )
        self.process_button.setEnabled(process_enabled)
        open_folder_enabled = bool(self.last_successful_output_dir) and (self.worker is None or not self.worker.isRunning())
        self.open_folder_button.setEnabled(open_folder_enabled)
        can_modify_inputs = (self.worker is None or not self.worker.isRunning()) and self.expected_file_count >= 0
        self.browse_file_button.setEnabled(can_modify_inputs)
        self.drop_area_frame.setEnabled(can_modify_inputs)
        # Also control the output dir button
        self.change_output_button.setEnabled(can_modify_inputs)
        # Add help button to state control
        self.help_button.setEnabled(can_modify_inputs)

    @Slot()
    def select_output_directory(self):
        """Slot for the 'Change...' button to select output directory."""
        new_dir = QFileDialog.getExistingDirectory(
            self,
            "Select Output Directory",
            self.output_dir_display.text() # Start browsing from current dir
        )
        if new_dir:
            # Only update if the directory actually changed
            if new_dir != self.output_dir:
                self.output_dir = new_dir
                self.output_dir_display.setText(new_dir)
                logging.info(f"Output directory set to: {self.output_dir}")
                # Allow reprocessing if output dir changes
                self.last_successful_output_dir = None
                self.update_ui_state() # Update button states

    @Slot()
    def start_processing(self):
        # Check files added
        if len(self.added_files) != self.expected_file_count or self.expected_file_count < 0:
            QMessageBox.critical(self, "Error", "Please ensure all required files are added.")
            return

        # Check output directory is set and valid
        if not self.output_dir or not os.path.isdir(self.output_dir):
            QMessageBox.critical(self, "Error", f"Please select a valid output directory.\nCurrent selection: '{self.output_dir}'")
            return

        logging.info(f"Processing files. Output directory: {self.output_dir}")
        self.log_text_edit.clear()
        self.process_button.setText("Processing...")
        self.last_successful_output_dir = None
        # *** NOTE: Worker now passes the dictionary of added files ***
        self.worker = ProcessingWorker(self.added_files.copy(), self.output_dir) # Pass selected directory
        self.worker.progress.connect(self.update_log)
        self.worker.finished.connect(self.processing_finished)
        self.worker.start()
        self.update_ui_state()

    @Slot(bool, str, str)
    def processing_finished(self, success, error_message, output_dir):
        self.process_button.setText("Process Files")
        # Update UI state *before* showing modal dialog
        self.update_ui_state()

        if success and output_dir:
            self.last_successful_output_dir = output_dir
            # Create and style the success message box
            msg_box = QMessageBox(self)
            msg_box.setWindowTitle("Success")
            msg_box.setIcon(QMessageBox.Icon.Information)
            msg_box.setText(f"Files processed successfully and saved to:\n{output_dir}")
            # Re-apply style
            msg_box.setStyleSheet("QMessageBox { background-color: #ffffff; color: #000000; } QLabel { background-color: #ffffff; color: #000000; }")
            msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
            msg_box.exec() # Modal call

            # Clear inputs after successful processing
            logging.info("Processing successful. Clearing input file list.")
            self.added_files.clear()
            self._update_file_status_display() # Update the list display

        else:
            self.last_successful_output_dir = None
            # Create and show error message box
            error_msg_box = QMessageBox(self)
            error_msg_box.setWindowTitle("Processing Error")
            error_msg_box.setIcon(QMessageBox.Icon.Critical)
            error_msg_box.setText(f"An error occurred during processing:\n{error_message}")
            # Optional: apply styling to error box too
            # error_msg_box.setStyleSheet("QMessageBox { background-color: #ffffff; color: #000000; } QLabel { background-color: #ffffff; color: #000000; }")
            error_msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
            error_msg_box.exec() # Modal call

        self.worker = None
        # Update UI state *after* modal dialog is closed
        self.update_ui_state()

    @Slot()
    def open_output_folder(self):
        if self.last_successful_output_dir and os.path.isdir(self.last_successful_output_dir):
            try:
                logging.info(f"Attempting to open output folder: {self.last_successful_output_dir}")
                if sys.platform == "win32":
                    os.startfile(self.last_successful_output_dir)
                elif sys.platform == "darwin": # macOS
                    subprocess.run(["open", self.last_successful_output_dir], check=True)
                else: # Linux and other Unix-like OS
                    subprocess.run(["xdg-open", self.last_successful_output_dir], check=True)
            except FileNotFoundError:
                 error_msg = f"Error: Could not find command to open folder on this system ({sys.platform})."
                 logging.error(error_msg)
                 QMessageBox.critical(self, "Error", error_msg)
            except subprocess.CalledProcessError as e:
                 error_msg = f"Error: Failed to open output folder ({e})."
                 logging.error(error_msg)
                 QMessageBox.critical(self, "Error", error_msg)
            except Exception as e:
                 error_msg = f"Could not open the output folder automatically:\n{self.last_successful_output_dir}\n\nError: {e}"
                 logging.error(f"Failed to open output folder: {e}")
                 QMessageBox.critical(self, "Error", error_msg)
        else:
            logging.warning("Attempted to open output folder, but no valid directory is set from last successful run.")
            QMessageBox.warning(self, "No Folder", "Output folder not found or not set from the last successful run.")

    @Slot()
    def show_input_file_help(self):
        """Loads mapping.csv and displays help about required inputs with samples."""
        # Determine the correct base path whether running as script or frozen exe
        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
            # Running as packaged executable
            # Use the temporary folder where PyInstaller extracted data
            base_path = sys._MEIPASS
        else:
            # Running as script
            # Use the directory containing this script file
            base_path = os.path.dirname(os.path.abspath(__file__))

        mapping_filename = 'mapping.csv'
        # Construct the full path to the mapping file
        mapping_filepath = os.path.join(base_path, mapping_filename)

        help_title = "Required Input File Information"
        # Use HTML for formatting
        # Reference the filename used in the message, not the potentially long full path
        html_content = f"<p>This tool processes data based on definitions in <b>{mapping_filename}</b>.</p>"
        html_content += "<p><b>Required file types, expected columns, and sample content:</b></p>"
        # Add note about case-insensitivity
        html_content += "<p><i>(Note: Column matching is case-insensitive)</i></p>"

        # Define static sample data (adjust as needed for representativeness)
        sample_data = {
            "Patient_Details": [
                {"PatientID": "1", "NHSNumber": "9990001111", "Age": "65", "GenderCode": "1", "Postcode": "AA1 1AA", "Ethnicity": "A", "LSOA": "E01000001", "PracticeCode": "P81001"},
                {"PatientID": "2", "NHSNumber": "9990002222", "Age": "42", "GenderCode": "2", "Postcode": "BB2 2BB", "Ethnicity": "M", "LSOA": "E01000002", "PracticeCode": "P81001"}
            ],
            "Care_History": [
                # Use a more realistic SNOMED code example
                {"PatientID": "1", "Code": "195967001", "CodeTerm": "Asthma", "EffectiveDate": "2020-01-15", "Value": "", "Unit": ""},
                {"PatientID": "1", "Code": "44054006", "CodeTerm": "Diabetes mellitus type 2", "EffectiveDate": "2022-03-10", "Value": "7.5", "Unit": "%"}
            ],
            "Medication_History": [
                {"PatientID": "1", "DrugCode": "12345001", "DrugName": "Aspirin Dispersible Tab 75mg", "IssueDate": "2023-01-10", "Quantity": "28", "Dosage": "Take ONE daily"},
                {"PatientID": "2", "DrugCode": "67890002", "DrugName": "Simvastatin Tab 40mg", "IssueDate": "2023-02-20", "Quantity": "28", "Dosage": "Take ONE at night"}
            ],
            "Long_Term_Conditions": [
                # Use a SNOMED code example for LTC
                {"PatientID": "1", "ConditionCode": "73211009", "ConditionName": "Hypertension", "OnsetDate": "2019-05-20", "ResolvedDate": ""},
                {"PatientID": "2", "ConditionCode": "195967001", "ConditionName": "Asthma", "OnsetDate": "2015-11-01", "ResolvedDate": ""}
            ]
        }

        try:
            if not os.path.exists(mapping_filepath):
                # Update error message to show the path it tried
                raise FileNotFoundError(f"Mapping file '{mapping_filename}' not found at expected location: {mapping_filepath}")

            mapping_df = pd.read_csv(mapping_filepath, dtype=str)
            required_mapping_cols = ['InputConfigKey', 'InputColumn']
            if not all(col in mapping_df.columns for col in required_mapping_cols):
                 raise ValueError("Mapping CSV is missing required InputConfigKey or InputColumn.")

            # Get unique InputConfigKeys that have at least one InputColumn specified
            valid_mappings = mapping_df.dropna(subset=['InputConfigKey', 'InputColumn'])
            valid_mappings = valid_mappings[valid_mappings['InputColumn'].astype(str).str.strip() != '']
            input_keys = valid_mappings['InputConfigKey'].unique()

            if not input_keys.any():
                 raise ValueError("No valid InputConfigKey/InputColumn pairs found in mapping CSV.")

            details_html = ""
            for key in sorted(input_keys):
                details_html += f"<p><b>{key.replace('_',' ')}:</b><br>"
                # Get unique, non-blank columns for this key
                key_mappings = valid_mappings[valid_mappings['InputConfigKey'] == key]
                columns = key_mappings['InputColumn'].unique()
                columns = [col for col in columns if col and str(col).strip() != '']
                columns_sorted = sorted(columns)

                if columns:
                    col_text = ", ".join(columns_sorted)
                    details_html += f"&nbsp;&nbsp;- Expected Columns: {col_text}<br>"
                else:
                    details_html += "&nbsp;&nbsp;- No specific input columns listed in mapping.csv.<br>"

                # Add sample data if available and columns were found
                if key in sample_data and columns:
                    try:
                        # Create sample DataFrame with only the expected columns
                        sample_df_data = sample_data[key]
                        # Filter sample dicts to only include keys that are in expected columns
                        filtered_sample_data = [
                            {k: row.get(k, '') for k in columns_sorted}
                            for row in sample_df_data
                        ]
                        sample_df = pd.DataFrame(filtered_sample_data)
                        # Ensure column order matches expected
                        sample_df = sample_df[columns_sorted]

                        sample_str = sample_df.to_string(index=False, header=True)
                        # Removed <br> here to reduce gap
                        details_html += "&nbsp;&nbsp;- Sample Content:"
                        details_html += f"<pre style='margin-left: 20px; background-color: #e9ecef; padding: 5px; border: 1px solid #ced4da;'>{sample_str}</pre>"
                    except Exception as sample_err:
                        logger.warning(f"Could not generate sample preview for {key}: {sample_err}")
                        details_html += "&nbsp;&nbsp;- <i>Could not generate sample preview.</i><br>"
                elif columns:
                     details_html += "&nbsp;&nbsp;- <i>No sample data defined for this type.</i><br>"

                details_html += "</p>" # Close paragraph for each key

            if not details_html:
                 details_html = "<p>Could not extract column details from mapping file.</p>"

            html_content += details_html

            # Create, style, and display QMessageBox
            msg_box = QMessageBox(self)
            msg_box.setWindowTitle(help_title)
            msg_box.setTextFormat(Qt.TextFormat.RichText) # Ensure HTML is parsed
            msg_box.setText(html_content)
            # Apply stylesheet for light background/dark text
            msg_box.setStyleSheet("QMessageBox { background-color: #ffffff; color: #000000; } QLabel { background-color: #ffffff; color: #000000; }")
            msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)
            msg_box.exec()

        except FileNotFoundError as e:
             # Create and show error message box directly
             QMessageBox.critical(self, "Error", str(e))
        except ValueError as e:
             QMessageBox.critical(self, "Mapping File Error", f"Could not parse requirements from '{mapping_filepath}'.\nError: {e}")
        except Exception as e:
             logger.exception(f"Unexpected error generating help text from '{mapping_filepath}'.")
             QMessageBox.critical(self, "Error", f"An unexpected error occurred reading '{mapping_filepath}':\n{e}")

    def _get_default_output_dir(self):
        """Determines the default output directory (Downloads folder)."""
        default_output_dir = ""
        try:
            if sys.platform == "win32":
                downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads')
                if os.path.isdir(downloads_path):
                     default_output_dir = downloads_path
            elif sys.platform == "darwin":
                 downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads')
                 if os.path.isdir(downloads_path):
                      default_output_dir = downloads_path
            else: # Assume Linux/other Unix-like
                 downloads_path = os.path.join(os.path.expanduser('~'), 'Downloads')
                 if os.path.isdir(downloads_path):
                      default_output_dir = downloads_path
        except Exception as e:
            logging.warning(f"Could not determine default Downloads directory: {e}")

        # Fallback to home directory if Downloads not found or error occurred
        if not default_output_dir or not os.path.isdir(default_output_dir):
            default_output_dir = os.path.expanduser('~')

        return default_output_dir

    def closeEvent(self, event):
        if self.worker and self.worker.isRunning():
            logging.warning("Application closing while processing is running.")
            pass
        event.accept()


# --- Application Entry Point ---
if __name__ == "__main__":
    app = QApplication(sys.argv)
    # Apply High DPI scaling
    # QApplication.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
    # QApplication.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)
    window = MainWindow()
    window.show()
    sys.exit(app.exec()) 