import requests
import pysolr
import pandas as pd
solr_url = "http://localhost:8983/solr/admin/collections"


def getDepFact(collection_name):
    emp=pysolr.Solr(f"http://localhost:8983/solr/{collection_name}/",always_commit=True)

    
    facet_query = {
        'facet': 'true',
        'facet.field': 'Gender', 
        'facet.mincount': 1,
        'rows': 0  
    }

    results = emp.search('*:*', **facet_query)

    if 'facet_counts' in results and 'facet_fields' in results['facet_counts']:
        gender_counts = results['facet_counts']['facet_fields'].get('Gender', [])
        if gender_counts:
            print("Employee Count by Gender:")
            for i in range(0, len(gender_counts), 2):
                print(f"{gender_counts[i]}: {gender_counts[i + 1]}")
        else:
            print("No counts found for the specified facet field.")
    else:
        print("No facet counts found.")
    
        
def delEmpById(collection_name,Emp_id):
    print("deleting please wait! \n")
    
    delemp=pysolr.Solr(f"http://localhost:8983/solr/{collection_name}/",always_commit=True)

    delete_query=f"Employee_ID:{Emp_id}"
 
    delemp.delete(delete_query)
    
    print("successfully Deleted \n")
    
def getEmpCount(collection_name):
    emp=pysolr.Solr(f"http://localhost:8983/solr/{collection_name}", always_commit=True)
    
    res=emp.search("*:*",row=0)
    
    print("Employe Count: ",res.hits)
    

def searchByColumn(collection_name,Column_name,Column_value):
    print("Searching please wait! \n")
    try:
        mysearch=pysolr.Solr(f"http://localhost:8983/solr/{collection_name}", always_commit=True)
        
        filter_queries={    
            Column_name:Column_value
        }
        query=[key+":"+val for key,val in filter_queries.items()]
        
        results=mysearch.search('*:*',fq=query)
        
        for result in results:
            print(result)
        
    except pysolr.SolrError as e:
        print(f"{Column_name} Field not found! \n")
  

def indexData(collection_name,Exclude_column):
    index=pysolr.Solr(f"http://localhost:8983/solr/{collection_name}", always_commit=True)
    
    mydata=pd.read_csv('data.csv', encoding='windows-1252')
    del mydata[Exclude_column]
    del mydata['Exit Date']
    
    documents=mydata.to_dict(orient='records')
    print("Indexing the document please wait! \n")
    index.add(documents)
    print("Process Successfully Completed \n")
   
def create_collection(collection_name):
    print(f"Creating collection: {collection_name} \n")
    try:
        
        params = {
            'action': 'CREATE', 
            'name': collection_name,
            'numShards': 1,                  
            'replicationFactor': 1,           
            'config': '_default',           
            'maxShardsPerNode': 1           
        }

        response = requests.post(solr_url, params=params)
        
        if response.status_code == 200:
            print("Collection created successfully!")
    except response.error:
        print("collection already exists!")

#creating collection
v_nameCollection = input("Enter V_nameCollection name: ")
v_phoneCollection = input("Enter V_phonecollection name: ")
create_collection(v_nameCollection)
create_collection(v_phoneCollection)

#Get Employee count
getEmpCount(v_nameCollection)

#Indexing data
indexData(v_nameCollection,'Department')
indexData(v_phoneCollection,'Gender')

#delete employe by id
delEmpById(v_nameCollection,'E02003')

#again get employee count
getEmpCount(v_nameCollection)


#search by column name
searchByColumn(v_nameCollection,'Department','IT')
searchByColumn(v_nameCollection,'Gender','Male')
searchByColumn(v_phoneCollection,'Department','IT')

# getDepFact(collection_name)