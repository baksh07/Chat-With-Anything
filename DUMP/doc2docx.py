from spire.doc import *
from spire.doc.common import *

# Create an object of the Document class
document = Document()
# Load a Word DOC file
document.LoadFromFile("/mnt/data/vocally_batch/DUMP/Free_Test_Data_100KB_DOC.doc")

# Save the DOC file to DOCX format
document.SaveToFile("/mnt/data/vocally_batch/DUMP/ToDocx.docx", FileFormat.Docx2016)
# Close the Document object
document.Close()