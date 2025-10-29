import os
import fitz  # PyMuPDF for PDF processing
import pandas as pd
from pathlib import Path
from docx import Document as DocxDocument
import hashlib
import re

def extract_text_with_metadata(file_path, filename):
    """Extract text with better structure and metadata"""
    text, error = process_file(file_path, filename)
    if error:
        return None, error
    
    enhanced_text = f"""
DOCUMENT: {filename}
CONTENT:
{text}
"""
    return enhanced_text.strip(), None

# ---------------- PDF HANDLING ----------------
def process_pdf_file(file_path):
    """Enhanced PDF processing with accurate page labeling"""
    try:
        full_text = ""
        doc = fitz.open(file_path)
        
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text = page.get_text()
            
            if text.strip():
                full_text += f"Page {page_num + 1}:\n{text}\n\n"
        
        doc.close()
        
        if not full_text or len(full_text.strip()) < 50:
            return None, "Document appears to be empty or contains no extractable text"
        return full_text, None
    except Exception as e:
        error_msg = f"Error processing PDF {file_path}: {str(e)}"
        print(error_msg)
        return None, error_msg

# ---------------- WORD HANDLING ----------------
def process_word_document(file_path):
    """Enhanced Word document processing with structure preservation"""
    try:
        doc = DocxDocument(file_path)
        full_text = ""
        
        for paragraph in doc.paragraphs:
            text = paragraph.text.strip()
            if not text:
                continue
            
            # Headings
            if paragraph.style and paragraph.style.name.startswith('Heading'):
                try:
                    level = int(paragraph.style.name.split()[-1]) if 'Heading' in paragraph.style.name else 1
                except:
                    level = 1
                header_prefix = "#" * min(level, 3) + " "
                full_text += f"\n{header_prefix}{text}\n"
                continue
            
            # Regular text
            full_text += text + "\n\n"
        
        # Tables
        for table in doc.tables:
            full_text += "\n[Table Start]\n"
            for row in table.rows:
                row_text = []
                for cell in row.cells:
                    cell_text = " ".join(par.text.strip() for par in cell.paragraphs)
                    row_text.append(cell_text)
                full_text += " | ".join(row_text) + "\n"
            full_text += "[Table End]\n\n"
        
        if not full_text or len(full_text.strip()) < 50:
            return None, "Document appears to be empty or contains minimal text"
        return full_text, None
    except Exception as e:
        error_msg = f"Error processing Word document {file_path}: {str(e)}"
        print(error_msg)
        return None, error_msg

# ---------------- EXCEL / CSV HANDLING ----------------
def process_excel_csv_file(file_path, filename):
    """Enhanced spreadsheet processing with 500 row limit"""
    file_ext = Path(file_path).suffix.lower()
    try:
        if file_ext == '.csv':
            df = pd.read_csv(file_path)
            return process_single_sheet_500_rows(df, filename), None
        elif file_ext in ['.xlsx', '.xls']:
            df_dict = pd.read_excel(file_path, sheet_name=None)
            if isinstance(df_dict, dict) and len(df_dict) > 1:
                return process_multiple_sheets_300_rows(df_dict, filename), None
            else:
                df = list(df_dict.values())[0] if isinstance(df_dict, dict) else df_dict
                return process_single_sheet_500_rows(df, filename), None
        else:
            return None, "Unsupported file type"
    except Exception as e:
        error_msg = f"Error processing spreadsheet: {str(e)}"
        print(error_msg)
        return None, error_msg

def process_single_sheet_500_rows(df, filename):
    """Process single sheet with 500 row limit"""
    text_content = f"## Document: {filename}\n\n"
    text_content += f"**Dimensions:** {df.shape[0]} rows × {df.shape[1]} columns\n"
    text_content += f"**Columns:** {', '.join(df.columns.tolist())}\n\n"
    
    # Set row limit to 500
    row_limit = 500
    total_rows = df.shape[0]
    
    text_content += "| " + " | ".join(df.columns) + " |\n"
    text_content += "|" + "|".join(["---"] * len(df.columns)) + "|\n"
    
    # Include up to 500 rows
    for _, row in df.head(row_limit).iterrows():
        text_content += "| " + " | ".join(str(x) for x in row.values) + " |\n"
    
    # Only show truncation message if we actually truncated
    if total_rows > row_limit:
        text_content += f"\n... and {total_rows - row_limit} more rows (showing {row_limit} of {total_rows} total rows)\n"
    
    return text_content

def process_multiple_sheets_300_rows(sheet_dict, filename):
    """Process multi-sheet workbook with 300 row limit per sheet"""
    text_content = f"# Workbook: {filename}\n\n"
    for sheet_name, df in sheet_dict.items():
        text_content += f"## Sheet: {sheet_name}\n"
        text_content += f"**Dimensions:** {df.shape[0]} rows × {df.shape[1]} columns\n"
        text_content += f"**Columns:** {', '.join(df.columns.tolist())}\n\n"
        
        # Set row limit to 300 per sheet for multi-sheet files
        row_limit = 300
        total_rows = df.shape[0]
        
        text_content += "| " + " | ".join(df.columns) + " |\n"
        text_content += "|" + "|".join(["---"] * len(df.columns)) + "|\n"
        
        for _, row in df.head(row_limit).iterrows():
            text_content += "| " + " | ".join(str(x) for x in row.values) + " |\n"
        
        if total_rows > row_limit:
            text_content += f"\n... and {total_rows - row_limit} more rows (showing {row_limit} of {total_rows} total rows)\n"
        
        text_content += "\n---\n\n"
    return text_content

# ---------------- TXT HANDLING ----------------
def process_text_file(file_path):
    """Process plain text files with structure preservation"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
            return content, None
    except Exception as e:
        error_msg = f"Error processing text file {file_path}: {str(e)}"
        print(error_msg)
        return None, error_msg

# ---------------- FILE HANDLER ----------------
def process_file(file_path, filename):
    """Process a file with enhanced type handling"""
    file_ext = Path(filename).suffix.lower()
    if file_ext == '.pdf':
        return process_pdf_file(file_path)
    elif file_ext == '.docx':
        return process_word_document(file_path)
    elif file_ext in ['.xlsx', '.xls', '.csv']:
        return process_excel_csv_file(file_path, filename)
    elif file_ext == '.txt':
        return process_text_file(file_path)
    else:
        return None, "Unsupported file type"

# ---------------- METADATA ----------------
def get_file_metadata(file_path):
    """Get metadata for a file"""
    return {
        "hash": calculate_file_hash(file_path),
        "size": os.path.getsize(file_path),
        "modified": os.path.getmtime(file_path)
    }

def calculate_file_hash(file_path):
    """Calculate MD5 hash of a file"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()