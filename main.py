import os
import nltk
import pandas as pd
from io import StringIO
#sent or word tokenize: Get the information into sentences or words
from nltk import sent_tokenize,word_tokenize
from nltk.corpus import stopwords
from nltk.probability import FreqDist
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

pathtohere=os.getcwd()
#nltk.download('stopwords')
#nltk.download('punkt')
"""
   Start ML with 10th period
"""
def main():
    print('Machine learning program.')
    print('1.Count words')
    print('2.Build a matrix')
    op=input()
    op=int(op)

    cloud_config= {

    'secure_connect_bundle': pathtohere+'//secure-connect-dbquart.zip'
         
    }
    
    objCC=CassandraConnection()
    auth_provider = PlainTextAuthProvider(objCC.cc_user,objCC.cc_pwd)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    querySt="select * from thesis.tbthesis where period_number=10 ALLOW FILTERING"   
        
    row=''
    

    if op==1:    
        ltDoc=StringIO()
    
        #Read and deliver a list of documents
        statement = SimpleStatement(querySt, fetch_size=1000)
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
            #ltDocuments.append(thesis)
            ltDoc.write(thesis+' ')
        
        #Complete words 
        words=word_tokenize(ltDoc.getvalue(),language='spanish')
        #Words without punctuation
        words_no_pun=[]
        for w in words:
            if w.isalpha():
                words_no_pun.append(w.lower())
        #Remove stopwords
        sw=stopwords.words('spanish')
        clean_words=[]
        for w in words_no_pun:
            if w not in sw:
                clean_words.append(w)


        #Clean words
        fdist=FreqDist(clean_words)
        
        fdist.plot(30)
        

    if op==2:
    
        ltDocuments=[]
        #Read and deliver a list of documents
        statement = SimpleStatement(querySt, fetch_size=1000)
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
            #ltDocuments.append(thesis)
            ltDoc.write(thesis+' ')

    
        sw=stopwords.words('spanish')
        cv=CountVectorizer(encoding='utf-8',stop_words=sw)
        df=pd.DataFrame()
        df=cv.fit_transform(ltDocuments).get_shape()
    

    print('...End...')   
    
    
     
        
    
class CassandraConnection():
    cc_user='quartadmin'
    cc_pwd='P@ssw0rd33'


if __name__=='__main__':
    main()