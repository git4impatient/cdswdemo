# make sure there is no .git directory in the CDSW tree
# or v1.4.0 will throw error
# failed to initiate the model build
#
# impyla install, based on some trial and error
# !git clone https://github.com/cloudera/impyla.git
# !pip install impyla   <- this fails
# !pip install argparse
# !pip install futures==3.0.3
# !pip uninstall -y impyla
# !pip install impyla   <- and now this works
#
# 
# here is the SQL you have been waiting for
from impala.dbapi import connect
conn = connect(host='ip-10-0-0-103.ec2.internal', port=21050)
#   172.28.210.3
cursor = conn.cursor()
#cursor.execute('SELECT * FROM sample_07p LIMIT 5') 

#print (cursor.description)  # prints the result set's schema
#results = cursor.fetchall()
#print (results)
#cursor.execute('select salary, total_emp from sample_07p where total_emp < 400000 limit 500')
#foo=300000*-0.0144608312694
def mylookup( args ):
  mykey=args["a"].split()
  print (mykey)
  cursor.execute("SELECT salary FROM sample_07p where code like ?", mykey )
  #db_cur.execute("UPDATE test_table SET field_1='%s' WHERE field_2='%s'" % (data, condition))
  # db_cur.execute("UPDATE test_table SET field_1=? WHERE field_2=?", data, condition)
  result=cursor.fetchall()
  return result
