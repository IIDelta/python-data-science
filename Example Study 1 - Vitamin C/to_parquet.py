import pandas as pd
import os
import pathlib
import tkinter as tk
from tkinter import filedialog

def convert_to_parquet(input_folder_path, output_folder_path=None):
    """
    Converts all .xlsx and .csv files in a specified folder to .parquet format.
    All data is initially read as strings to prevent type inference errors.

    For .xlsx files with multiple sheets, each sheet is saved as a separate .parquet file,
    with the sheet name appended. If an .xlsx file has only ONE sheet, the sheet name
    is OMITTED from the output filename.

    Args:
        input_folder_path (str): The path to the folder containing the source files.
        output_folder_path (str, optional): The path to the folder where .parquet files
                                            will be saved. If None, saves them in the
                                            same directory as the input files.
                                            Defaults to None.
    """
    if not input_folder_path: # Check if a path was actually selected
        print("No input folder selected. Exiting.")
        return

    input_path = pathlib.Path(input_folder_path)
    if not input_path.is_dir():
        print(f"Error: Input path '{input_folder_path}' is not a valid directory.")
        return

    if output_folder_path:
        output_path = pathlib.Path(output_folder_path)
        try:
            output_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Error creating output directory '{output_path}': {e}")
            return
    else:
        output_path = input_path

    print(f"Scanning folder: {input_path}")
    print(f"Parquet files will be saved to: {output_path}")
    print("Note: All data will be read as strings to ensure robust conversion.")

    converted_count = 0
    error_count = 0

    for file_path in input_path.iterdir():
        if file_path.is_file():
            original_filename_stem = file_path.stem # Filename without extension
            sheet_name_for_error = "N/A" # Initialize for error reporting
            
            try:
                if file_path.suffix.lower() == '.xlsx':
                    print(f"\nProcessing Excel file: {file_path.name}")
                    xls = pd.ExcelFile(str(file_path))
                    sheet_names = xls.sheet_names
                    
                    if not sheet_names:
                        print(f"  Warning: No sheets found in {file_path.name}.")
                        continue

                    # Check if there is only one sheet
                    is_single_sheet = len(sheet_names) == 1

                    for sheet_name in sheet_names:
                        sheet_name_for_error = sheet_name # Update for current sheet
                        print(f"  Converting sheet: '{sheet_name}'...")
                        df = xls.parse(sheet_name, dtype=str) 
                        
                        # --- NEW NAMING LOGIC ---
                        if is_single_sheet:
                            parquet_filename = f"{original_filename_stem}.parquet"
                        else:
                            parquet_filename = f"{original_filename_stem}_{sheet_name}.parquet"
                        # --- END NEW NAMING LOGIC ---
                            
                        output_file_path = output_path / parquet_filename
                        
                        df.to_parquet(output_file_path, engine='pyarrow', index=False)
                        print(f"    Successfully converted '{sheet_name}' to '{output_file_path.name}'")
                        converted_count += 1

                elif file_path.suffix.lower() == '.csv':
                    sheet_name_for_error = "N/A" # Reset for CSV
                    print(f"\nProcessing CSV file: {file_path.name}")
                    df = pd.read_csv(str(file_path), dtype=str) 
                    
                    parquet_filename = f"{original_filename_stem}.parquet"
                    output_file_path = output_path / parquet_filename
                    
                    df.to_parquet(output_file_path, engine='pyarrow', index=False)
                    print(f"  Successfully converted to '{output_file_path.name}'")
                    converted_count += 1
                
            except Exception as e:
                print(f"  Error processing file {file_path.name} (sheet: {sheet_name_for_error}): {e}")
                error_count += 1

    print(f"\n--- Conversion Summary ---")
    print(f"Total files/sheets converted successfully: {converted_count}")
    print(f"Total errors encountered: {error_count}")

if __name__ == "__main__":
    root = tk.Tk()
    root.withdraw() 

    print("--- Excel/CSV to Parquet Converter ---")
    
    print("Please select the folder containing your .xlsx or .csv files.")
    input_folder = filedialog.askdirectory(title="Select Input Folder")

    if not input_folder: 
        print("No input folder selected. Exiting.")
    else:
        print(f"Input folder selected: {input_folder}")
        
        specify_output_console = input("Do you want to save Parquet files to a different folder? (yes/no, default: no): ").strip().lower()
        
        output_folder = None
        if specify_output_console == 'yes':
            print("Please select the folder where Parquet files will be saved.")
            output_folder = filedialog.askdirectory(title="Select Output Folder")
            if not output_folder: 
                print("No output folder selected, Parquet files will be saved in the input folder.")
                output_folder = None 
            else:
                print(f"Output folder selected: {output_folder}")
        
        convert_to_parquet(input_folder, output_folder)
