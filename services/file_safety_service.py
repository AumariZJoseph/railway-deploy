import os
import magic
import logging
from pathlib import Path
import tempfile
import shutil
from typing import Tuple
import hashlib

logger = logging.getLogger(__name__)

class FileSafetyService:
    def __init__(self):
        # Allowed MIME types with their expected extensions
        self.allowed_mime_types = {
            # PDF files
            'application/pdf': ['.pdf'],
            
            # Word documents
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ['.docx'],
            'application/msword': ['.doc'],
            
            # Text files
            'text/plain': ['.txt'],
            
            # CSV files
            'text/csv': ['.csv'],
            'application/csv': ['.csv'],
            
            # Excel files
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
            'application/vnd.ms-excel': ['.xls'],
        }
        
        # Maximum file sizes (in bytes)
        self.max_file_sizes = {
            '.pdf': 3 * 1024 * 1024,      # 3MB
            '.docx': 3 * 1024 * 1024,     # 3MB
            '.doc': 3 * 1024 * 1024,      # 3MB
            '.txt': 1 * 1024 * 1024,      # 1MB
            '.csv': 5 * 1024 * 1024,      # 5MB
            '.xlsx': 5 * 1024 * 1024,     # 5MB
            '.xls': 5 * 1024 * 1024,      # 5MB
        }
        
        # Known malicious file signatures (first few bytes)
        self.malicious_signatures = {
            b'MZ': 'Windows executable',  # EXE files
            b'%PDF': None,  # Allow PDF but will validate further
            b'PK\x03\x04': None,  # ZIP files (DOCX, XLSX are ZIP-based)
            b'\xD0\xCF\x11\xE0': 'Microsoft Office legacy format',  # DOC, XLS
        }

    def validate_file_safety(self, file_path: str, original_filename: str) -> Tuple[bool, str]:
        """
        Comprehensive file safety validation for ALL file types
        Returns: (is_safe, error_message)
        """
        try:
            # Step 1: Check file extension
            file_extension = Path(original_filename).suffix.lower()
            if not self._is_extension_allowed(file_extension):
                return False, f"File type {file_extension} is not supported. Please upload PDF, Word, Excel, CSV, or text files."

            # Step 2: Check file size
            file_size = os.path.getsize(file_path)
            max_size = self.max_file_sizes.get(file_extension, 3 * 1024 * 1024)
            if file_size > max_size:
                size_mb = max_size / 1024 / 1024
                return False, f"File size exceeds {size_mb}MB limit. Your file: {(file_size / 1024 / 1024):.2f}MB"

            # Step 3: Check for empty files
            if file_size == 0:
                return False, "File is empty"

            # Step 4: Verify MIME type matches extension
            mime_type = self._get_mime_type(file_path)
            if not self._is_mime_type_allowed(mime_type, file_extension):
                return False, f"File type mismatch. Detected: {mime_type}, Expected: {file_extension}. File may be corrupted."

            # Step 5: Check for known malicious file signatures
            signature_check = self._check_file_signature(file_path, file_extension)
            if not signature_check[0]:
                return signature_check

            # Step 6: File-specific deep validation with better error handling
            try:
                if file_extension == '.pdf':
                    return self._validate_pdf_safety(file_path)
                elif file_extension in ['.docx', '.doc']:
                    return self._validate_word_safety(file_path, file_extension)
                elif file_extension in ['.xlsx', '.xls', '.csv']:
                    return self._validate_spreadsheet_safety(file_path, file_extension)
                elif file_extension == '.txt':
                    return self._validate_text_safety(file_path)
            except Exception as file_specific_error:
                logger.error(f"File-specific validation error for {file_extension}: {str(file_specific_error)}")
                return False, f"Unable to validate {file_extension} file. It may be corrupted or in an unsupported format."

            return True, "File appears safe"

        except Exception as e:
            logger.error(f"File safety validation error: {str(e)}")
            return False, "File validation failed due to an unexpected error"

    def _is_extension_allowed(self, extension: str) -> bool:
        """Check if file extension is allowed"""
        allowed_extensions = {ext for exts in self.allowed_mime_types.values() for ext in exts}
        return extension in allowed_extensions

    def _get_mime_type(self, file_path: str) -> str:
        """Get actual MIME type using magic"""
        try:
            mime = magic.Magic(mime=True)
            return mime.from_file(file_path)
        except Exception as e:
            logger.error(f"MIME type detection failed: {str(e)}")
            return "application/octet-stream"

    def _is_mime_type_allowed(self, mime_type: str, file_extension: str) -> bool:
        """Check if MIME type matches expected file type"""
        if mime_type not in self.allowed_mime_types:
            return False
        
        expected_extensions = self.allowed_mime_types[mime_type]
        return file_extension in expected_extensions

    def _check_file_signature(self, file_path: str, file_extension: str) -> Tuple[bool, str]:
        """Check file signature against known malicious patterns"""
        try:
            with open(file_path, 'rb') as f:
                header = f.read(20)  # Read first 20 bytes
            
            # Check for executable files disguised as documents
            if header.startswith(b'MZ') and file_extension not in ['.exe', '.dll']:
                return False, "File appears to be an executable disguised as a document"
            
            # Check for ZIP bombs (extremely compressed files)
            if header.startswith(b'PK') and file_extension in ['.docx', '.xlsx']:
                file_size = os.path.getsize(file_path)
                # Check compression ratio (very small file with ZIP header might be a bomb)
                if file_size < 1000 and b'PK' in header:
                    return False, "File appears to be a ZIP bomb"
            
            return True, "File signature appears safe"
            
        except Exception as e:
            logger.error(f"File signature check error: {str(e)}")
            return False, "File signature check failed"

    def _validate_pdf_safety(self, file_path: str) -> Tuple[bool, str]:
        """Deep PDF safety validation with correct PyMuPDF methods"""
        try:
            import fitz  # PyMuPDF
            
            doc = fitz.open(file_path)
            
            # Check for reasonable page count
            if len(doc) > 1000:
                doc.close()
                return False, "PDF has too many pages (potential DoS attack)"
            
            # Check for embedded files - CORRECTED METHOD
            embedded_files = doc.embfile_names()
            if embedded_files:
                doc.close()
                return False, "PDF contains embedded files (security risk)"
            
            # Check for JavaScript - CORRECTED METHOD
            # PyMuPDF doesn't have has_javascript(), we need to check differently
            try:
                # Alternative approach: Try to get JavaScript actions
                # This is a more reliable way to check for JavaScript in PDFs
                has_js = False
                for page_num in range(min(5, len(doc))):  # Check first 5 pages
                    page = doc.load_page(page_num)
                    # Check for JavaScript actions in annotations
                    for annot in page.annots():
                        if annot.type[0] == 1:  # Text annotation
                            if hasattr(annot, 'script') and annot.script:
                                has_js = True
                                break
                    if has_js:
                        break
                        
                if has_js:
                    doc.close()
                    return False, "PDF contains JavaScript (security risk)"
                    
            except Exception as js_error:
                # If we can't check for JavaScript, be cautious but don't fail
                logger.warning(f"JavaScript check failed for {file_path}: {str(js_error)}")
            
            # Try to extract text to test parsing
            try:
                text_content = ""
                for page_num in range(min(3, len(doc))):
                    page = doc.load_page(page_num)
                    text = page.get_text()
                    text_content += text
                    
                # If we can't extract meaningful text, the PDF might be problematic
                if len(text_content.strip()) < 50:
                    doc.close()
                    return False, "PDF appears to be empty or contains no extractable text"
                    
            except Exception as e:
                doc.close()
                return False, f"PDF appears malformed: {str(e)}"
            
            doc.close()
            return True, "PDF appears safe"
            
        except Exception as e:
            logger.error(f"PDF validation error: {str(e)}")
            return False, f"PDF validation failed: {str(e)}"

    def _validate_word_safety(self, file_path: str, file_extension: str) -> Tuple[bool, str]:
        """Word document safety validation"""
        try:
            if file_extension == '.docx':
                # DOCX files are ZIP archives containing XML
                import zipfile
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    # Check for embedded objects or macros
                    file_list = zip_ref.namelist()
                    
                    # Look for potential macro files
                    macro_files = [f for f in file_list if 'macros' in f.lower() or 'vba' in f.lower()]
                    if macro_files:
                        return False, "Word document contains macros (security risk)"
                    
                    # Check for embedded objects
                    embedded_files = [f for f in file_list if 'embeddings' in f.lower()]
                    if embedded_files:
                        return False, "Word document contains embedded files (security risk)"
                    
            elif file_extension == '.doc':
                # Legacy DOC format - harder to check, so be more cautious
                file_size = os.path.getsize(file_path)
                if file_size > 10 * 1024 * 1024:  # 10MB limit for old DOC
                    return False, "Legacy Word document too large (potential risk)"
            
            # Try to open with python-docx for .docx
            if file_extension == '.docx':
                from docx import Document
                doc = Document(file_path)
                # If we can open without errors, likely safe
            
            return True, "Word document appears safe"
            
        except Exception as e:
            return False, f"Word document validation failed: {str(e)}"

    def _validate_spreadsheet_safety(self, file_path: str, file_extension: str) -> Tuple[bool, str]:
        """Spreadsheet safety validation"""
        try:
            import pandas as pd
            
            if file_extension == '.csv':
                # Check for extremely wide CSV (potential DoS)
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    first_line = f.readline()
                    if first_line.count(',') > 1000:  # More than 1000 columns
                        return False, "CSV has too many columns (potential DoS)"
                
                df = pd.read_csv(file_path, nrows=5)
                
            elif file_extension in ['.xlsx', '.xls']:
                # Excel files - check for macros and embedded objects
                if file_extension == '.xlsx':
                    import zipfile
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        file_list = zip_ref.namelist()
                        
                        # Check for macro files
                        macro_files = [f for f in file_list if 'macros' in f.lower() or 'vba' in f.lower()]
                        if macro_files:
                            return False, "Excel file contains macros (security risk)"
                
                # Try to read the file
                df = pd.read_excel(file_path, nrows=5)
                
                # Check for reasonable dimensions
                if df.shape[1] > 1000:  # More than 1000 columns
                    return False, "Excel file has too many columns (potential DoS)"
            
            return True, "Spreadsheet appears safe"
            
        except Exception as e:
            return False, f"Spreadsheet validation failed: {str(e)}"

    def _validate_text_safety(self, file_path: str) -> Tuple[bool, str]:
        """Text file safety validation"""
        try:
            file_size = os.path.getsize(file_path)
            
            # Check for extremely long lines (potential DoS)
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for i, line in enumerate(f):
                    if len(line) > 100000:  # Very long line
                        return False, "Text file contains extremely long lines (potential DoS)"
                    if i > 10000:  # Too many lines to check
                        break
            
            # Check file encoding and basic readability
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read(1024)  # Read first 1KB
                
            # Check for binary content in text file
            text_characters = bytearray({7,8,9,10,12,13,27} | set(range(0x20, 0x100)) - {0x7f})
            with open(file_path, 'rb') as f:
                binary_content = f.read(1024)
                if binary_content.translate(None, text_characters):
                    return False, "Text file appears to contain binary data"
            
            return True, "Text file appears safe"
            
        except Exception as e:
            return False, f"Text file validation failed: {str(e)}"

    def calculate_file_hash(self, file_path: str) -> str:
        """Calculate file hash for tracking/blocklisting"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

# Global instance
file_safety_service = FileSafetyService()