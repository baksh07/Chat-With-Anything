from spire.doc import *
from spire.doc.common import *
from pathlib import Path

# Create an object of the Document class
document = Document()
dump_dir = Path(__file__).resolve().parent
# Load a Word DOC file
document.LoadFromFile(str(dump_dir / "Free_Test_Data_100KB_DOC.doc"))

# Save the DOC file to DOCX format
document.SaveToFile(str(dump_dir / "ToDocx.docx"), FileFormat.Docx2016)
# Close the Document object
document.Close()