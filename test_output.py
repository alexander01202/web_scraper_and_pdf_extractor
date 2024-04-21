"""
File to test that the output json is properly formatted.

See use in `run.sh`.

Runnable from the command line as `python test_output.py $JSON_FILE`
"""


import sys
import json
from datetime import datetime
import zlib
import base64

def decode(data: str) -> bytes:
  """ Return decoded value of a compressed base-64 encoded string """ 
  return zlib.decompress(base64.b64decode(data))

def validate_json(filename: str):
  with open(filename, 'r', encoding='utf-8') as f:
    data = json.load(f)
    assert 'metadata' in data, "Data must have metadata key"
    assert 'errors' in data, "Data must have errors key"
    assert 'successes' in data, "Data must have sucesses key"

    date_format = '%Y-%m-%d'
    assert data['metadata']['schema'] == 'v2'
    
    start_date = data['metadata']['query_start_date']
    assert datetime.strptime(start_date, date_format) < datetime.utcnow(), "start date must be before now"

    end_date = data['metadata']['query_end_date']
    end_date_dt = datetime.strptime(end_date, date_format)
    assert end_date_dt, "end date must exist"

    for success in data['successes']:
      assert 'datetime_accessed' in success, "Must contain datetime_accessed key"
      assert len(success['language']) == 2, "Must use 2 digit language code"
      assert success['document_type'] in ['speech', 'interview', 'press_release'], "Document type must be one of: 'speech', 'interview', 'press_release'"
      
      assert 'document_author' in success, "Must contain document_author key"

      doc_date = datetime.strptime(success['document_date'], date_format)
      assert doc_date, f"document date must be in format {date_format}"

      assert 'document_title' in success, "Must contain document_title key"

      
      assert 'document_text' in success and len(success['document_text']), "document must have text"
      assert 'document_url' in success and len(success['document_url']), "document must have a url"
      assert 'document_html' in success, "Must contain document_html key. Leave blank if no html given"
      assert 'document_pdf_encoded' in success, "Must contain document_pdf_encoded key. Leave blank if no pdf found"
        
      if len(success['document_pdf_encoded']):
        doc_pdf = decode(success['document_pdf_encoded'])
        assert doc_pdf, "If a document has a PDF, it must be compressed"
      
      assert 'document_tables' in success, "Must contain document_tables key. Leave a blank array [] if not a pdf or if no tables extracted from pdf"
    
    print("OK")

if __name__ == '__main__': 
  filename = sys.argv[1]
  validate_json(filename)
