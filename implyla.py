# connect to impala and run a simple query
# plot result set
! git clone https://github.com/cloudera/thrift_sasl
!python ~/thrift_sasl/setup.py build
!pip uninstall  thrift_sasl/
!pip install -e thrift_sasl/
!pip install sasl
!pip install impyla
!pip install  thrift
!pip show thrift

#!pip install thrift==0.9.3

from impala.dbapi import connect
conn = connect(host='10.142.0.2', port=21050)
cursor = conn.cursor()
cursor.execute('SELECT * FROM sample_07p LIMIT 10')
print cursor.description  # prints the result set's schema
results = cursor.fetchall()
print results
cursor.execute('select salary, total_emp from sample_07p where total_emp < 400000 limit 500')
foo=300000*-0.0144608312694
def plotit():
 for row in cursor:
    #print row[0] , row[1]
    plt.scatter(row[1],row[0])
 plt.plot([0, 300000], [50023, foo])
 plt.show()
plotit()
print "done"
