!pip uninstall -y sasl
!pip uninstall -y impyla
!pip uninstall  -y thrift
!pip uninstall -y thrift_sasl/
!pip install -e thrift_sasl/
!pip install thrift==0.9.3
!pip install impyla==0.13.0
!pip show thrift
from impala.dbapi import connect
conn = connect(host='10.142.0.2', port=21050)
cursor = conn.cursor()
cursor.execute('SELECT * FROM sample_07p LIMIT 10')                                                  
