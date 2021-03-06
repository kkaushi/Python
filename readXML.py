import sys
import mysql.connector
import re
from mysql.connector import errorcode
from lxml import etree

def char_replace(str):
    temp_str=str
    temp_char='\''
    temp_char2='\\';
    if temp_char in temp_str:
        new_str=temp_str.replace(temp_char,'');
        if temp_char2 in new_str:
            new_str2=new_str.replace(temp_char2,'');
            return new_str2
        else:
            return new_str
    elif temp_char2 in temp_str:
        new_str=temp_str.replace(temp_char2,'')
        return new_str
    else:
        return str
    
def fast_iter(context,conn,dbcursor,ctr, func):
    quesCount=0;
    for event, elem in context:
        if elem.xpath("./maincat"):
            maincat=elem.xpath("./*[local-name()='maincat']/text()")[0]
            #print maincat
            if(maincat=='Health'):
                #print "ctr: "+str(ctr)
                func(conn,dbcursor,elem)
                quesCount+=1;
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            #if ctr%100000==0:
            #   print 'No of health questions: ',quesCount
        ctr+=1
    print 'No of questions: ',quesCount
    del context

def process_element(conn,dbcursor,elem):
    temp_document_type=''
    temp_uri=''
    temp_subject=''
    temp_content=''
    temp_bestanswer=''
    temp_cat=''
    temp_maincat=''
    temp_subcat=''
    temp_date=''
    temp_res_date=''
    temp_vot_date=''
    temp_lastanswerts=''
    temp_qlang=''
    temp_qintl=''
    temp_language=''
    temp_id=''
    temp_best_id=''
    temp_ans_item=[]
    if elem.xpath("./@type"):
        temp_document_type=elem.xpath("./@type")[0]
        #print "document type: ",temp_document_type.encode(sys.stdout.encoding, errors='replace')                                
    if elem.xpath("./uri"):
        temp_uri=elem.xpath("./*[local-name()='uri']/text()")[0]
        #print "uri: ",temp_uri.encode(sys.stdout.encoding, errors='replace')                                
    if elem.xpath("./subject"):
        temp_subject=elem.xpath("./*[local-name()='subject']/text()")[0]	
        #print "subject: ",temp_subject.encode(sys.stdout.encoding, errors='replace')
    if elem.xpath("./content"):
        temp_content=elem.xpath("./*[local-name()='content']/text()")[0]	
        #print "content: ",temp_content.encode(sys.stdout.encoding, errors='replace')
    if elem.xpath("./bestanswer"):
        temp_bestanswer=elem.xpath("./*[local-name()='bestanswer']/text()")[0]
        #print "best answer: ",temp_bestanswer.encode(sys.stdout.encoding, errors='replace')
    for element in elem.iter("answer_item"):
        temp_answer_item=element.text
        temp_ans_item.append(temp_answer_item)
        #print "answer item: "+temp_answer_item.encode(sys.stdout.encoding, errors='replace')                  
    if elem.xpath("./cat"):
        temp_cat=elem.xpath("./*[local-name()='cat']/text()")[0]	
        #print "cat: ",temp_cat.encode(sys.stdout.encoding, errors='replace')
    if elem.xpath("./maincat"):
        temp_maincat=elem.xpath("./*[local-name()='maincat']/text()")[0]	
        #print "main cat: ",temp_maincat.encode(sys.stdout.encoding, errors='replace')
    if elem.xpath("./subcat"):
        temp_subcat=elem.xpath("./*[local-name()='subcat']/text()")[0]	
        #print "sub cat: ",temp_subcat.encode(sys.stdout.encoding, errors='replace')
    if elem.xpath("./date"):
        temp_date=elem.xpath("./*[local-name()='date']/text()")[0]	
        #print "date: ",temp_date.encode(sys.stdout.encoding, errors='replace')
    if elem.xpath("./res_date"):
        temp_res_date=elem.xpath("./*[local-name()='res_date']/text()")[0]	
        #print "result date: ",temp_res_date.encode(sys.stdout.encoding, errors='replace')
    if elem.xpath("./vot_date"):
        temp_vot_date=elem.xpath("./*[local-name()='vot_date']/text()")[0]	
        #print "vote date: ",temp_vot_date.encode(sys.stdout.encoding, errors='replace')
    if elem.xpath("./lastanswerts"):
        temp_lastanswerts=elem.xpath("./*[local-name()='lastanswerts']/text()")[0]	
        #print "last answerts: ",temp_lastanswerts.encode(sys.stdout.encoding, errors='replace')
    if elem.xpath("./qlang"):
        temp_qlang=elem.xpath("./*[local-name()='qlang']/text()")[0]
        #print "q lang: ",temp_qlang.encode(sys.stdout.encoding, errors='replace')	
    if elem.xpath("./qintl"):
        temp_qintl=elem.xpath("./*[local-name()='qintl']/text()")[0]	
        #print "q intl: ",temp_qintl.encode(sys.stdout.encoding, errors='replace')
    if elem.xpath("./language"):
        temp_language=elem.xpath("./*[local-name()='language']/text()")[0]	
        #print "language: ",temp_language.encode(sys.stdout.encoding, errors='replace')
    if elem.xpath("./id"):
        temp_id=elem.xpath("./*[local-name()='id']/text()")[0]
        #print "id: ",temp_id.encode(sys.stdout.encoding, errors='replace')	
    if elem.xpath("./best_id"):
        temp_best_id=elem.xpath("./*[local-name()='best_id']/text()")[0]	
        #print "best id: ",temp_best_id.encode(sys.stdout.encoding, errors='replace')
    
    str_document_type=char_replace(temp_document_type)
    str_uri=char_replace(temp_uri)
    str_subject=char_replace(temp_subject)
    str_content=char_replace(temp_content)
    str_bestanswer=char_replace(temp_bestanswer)
    str_cat=char_replace(temp_cat)
    str_maincat=char_replace(temp_maincat)
    str_subcat=char_replace(temp_subcat)
    str_date=char_replace(temp_date)
    str_res_date=char_replace(temp_res_date)
    str_vot_date=char_replace(temp_vot_date)
    str_lastanswerts=char_replace(temp_lastanswerts)
    str_qlang=char_replace(temp_qlang)
    str_qintl=char_replace(temp_qintl)
    str_language=char_replace(temp_language)
    str_id=char_replace(temp_id)
    str_best_id=char_replace(temp_best_id)
    insert_query="INSERT INTO maintable VALUES ('%s', '%s', '%s', '%s','%s', '%s', '%s', '%s','%s', '%s', '%s', '%s','%s', '%s', '%s', '%s','%s')" %(str_document_type,str_uri,str_subject,str_content,str_bestanswer,str_cat,str_maincat,str_subcat,str_date,str_res_date,str_vot_date,str_lastanswerts,str_qlang,str_qintl,str_language,str_id,str_best_id)
    #print insert_query  
    dbcursor.execute(insert_query)
    conn.commit()
    #print hashstring
    for i in temp_ans_item:
        #print i.encode(sys.stdout.encoding,errors='replace')
        str_uri=char_replace(temp_uri)
        str_i=char_replace(i)
        insert_answer_query="INSERT INTO answerstable VALUES ('%s', '%s')" %(str_uri,str_i)
        dbcursor.execute(insert_answer_query)
        conn.commit()
    #print hashstring
				

#Create Database Connection
try:
    conn=mysql.connector.connect(user='root',password='',host='localhost',database='yahoolabsdata');
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exists")
    else:
        print(err)
else:
    dbcursor=conn.cursor()
    hashstring='---------------------------------------------------------';
    context=etree.iterparse('D:\Part time\Yahoo health\yahoo labs data\FullOct2007.xml', events=('end',), tag='document')
    ctr=0;    
    fast_iter(context,conn,dbcursor,ctr,process_element)
    conn.close()
