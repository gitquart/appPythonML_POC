import os
import nltk
#sent or word tokenize: Get the information into sentences or words
from nltk import sent_tokenize,word_tokenize
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

pathtohere=os.getcwd()

def main():
    
    cloud_config= {

        'secure_connect_bundle': pathtohere+'//secure-connect-dbquart.zip'
         
    }
    
    objCC=CassandraConnection()
   
    auth_provider = PlainTextAuthProvider(objCC.cc_user,objCC.cc_pwd)
    
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    print('ML with python')
    





class CassandraConnection():
    cc_user='quartadmin'
    cc_pwd='P@ssw0rd33'


if __name__=='__main__':
    main()