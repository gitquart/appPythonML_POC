import os
import nltk
import pandas as pd
from io import StringIO
#sent or word tokenize: Get the information into sentences or words
from nltk import sent_tokenize,word_tokenize
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

pathtohere=os.getcwd()
#nltk.download('stopwords')
"""
   Start ML with 10th period
"""
def main():
    
    
    cloud_config= {

        'secure_connect_bundle': pathtohere+'//secure-connect-dbquart.zip'
         
    }
    
    objCC=CassandraConnection()
   
    auth_provider = PlainTextAuthProvider(objCC.cc_user,objCC.cc_pwd)
    
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    
    

    querySt="select * from thesis.tbthesis where period_number=10 ALLOW FILTERING"   
        
    row=''
    statement = SimpleStatement(querySt, fetch_size=1000)
    ltDocuments=[]

    for row in session.execute(statement):
        thesis_b=StringIO()
        for col in row:
            if type(col) is list:
                for e in col:
                    thesis_b.write(str(e)+' ')
            else:        
                thesis_b.write(str(col)+' ')
        thesis=''
        thesis=thesis_b.getvalue()
        ltDocuments.append(thesis)

    sw=stopwords.words('spanish')
    cv=CountVectorizer(encoding='utf-8',stop_words=sw)
    cv.fit_transform(ltDocuments).get_shape()

     
        
    
class CassandraConnection():
    cc_user='quartadmin'
    cc_pwd='P@ssw0rd33'


if __name__=='__main__':
    main()