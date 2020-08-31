import os
import nltk
#sent or word tokenize: Get the information into sentences or words
from nltk import sent_tokenize,word_tokenize
from nltk.corpus import stopwords
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

pathtohere=os.getcwd()
#nltk.download('stopwords')
def main():
    
    cloud_config= {

        'secure_connect_bundle': pathtohere+'//secure-connect-dbquart.zip'
         
    }
    
    objCC=CassandraConnection()
   
    auth_provider = PlainTextAuthProvider(objCC.cc_user,objCC.cc_pwd)
    
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    #session = cluster.connect()
    
    sw=stopwords.words('spanish')
    print(sw)
    print('ML with python')

    querySt="select * from test.tbthesis where period_number>4 ALLOW FILTERING "   
        
    count=0
    row=''
    statement = SimpleStatement(querySt, fetch_size=1000)
    
    for row in session.execute(statement):
        count=count+1
        
    
class CassandraConnection():
    cc_user='quartadmin'
    cc_pwd='P@ssw0rd33'


if __name__=='__main__':
    main()