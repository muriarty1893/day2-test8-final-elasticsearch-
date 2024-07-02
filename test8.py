#!/usr/bin/env python
# coding: utf-8
######03.07.2024
# In[28]:


from elasticsearch import Elasticsearch, helpers
import pandas as pd


# In[29]:


es=Elasticsearch([{"host":"localhost","port":9200}],http_auth=("elastic","suVlWmYRKKFk6RYs_TrU"))
print(es.ping())
#index creation in bulk using input file


# In[30]:


df= pd.read_csv("https://github.com/erkansirin78/datasets/raw/master/spark_book_data/retail-data/all/online-retail-dataset.csv")
df.head()


# In[31]:


df.dropna(inplace=True)


# In[32]:


online_retail_index =  {
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "custom_analyzer":
          {
            "type":"custom",
            "tokenizer":"standard",
            "filter":[
              "lowercase", "custom_edge_ngram","asciifolding"
            ]
          }
        },
        "filter": {
          "custom_edge_ngram": {
            "type": "edge_ngram",
            "min_gram":2,
            "max_gram": 10
            }
          }
        }
      }
    },
    "mappings": {
    "properties": {
      "InvoiceNo":    { "type": "keyword" },  
      "StockCode":  { "type": "keyword"  }, 
      "Description":   { "type": "text"  },
      "Quantity": {"type": "integer"},
      "InvoiceDate": {
        "type":   "date",
        "format": "MM/dd/yyyy hh:ss"
      },
      "UnitPrice": {"type": "float"},
      "CustomerID": {"type": "keyword"},
      "Country": {"type": "keyword"}
    }
  }
  }


# In[33]:


def dataframe_to_es(df, es_index):
    for df_idx, line in df.iterrows():
        yield {
            "_index": es_index,
            "_id":df_idx,
            "_source" : {
                "InvoiceNo": line[0],
                "StockCode": line[1],
                "Description": line[2],
                "Quantity": line[3],
                "InvoiceDate": line[4],
                "UnitPrice": line[5],
                "CustomerID": line[6],
                "Country": line[7]
            }
        }


# In[40]:


try:
    es.indices.delete("online_retail_python")
except:
    print("No index")


# In[41]:


helpers.bulk(es, dataframe_to_es(df, "online_retail_python"), raise_on_error=False)


# In[42]:


keyword = "Coffee"
res = es.search(index='online_retail_python', body={
    "query": {
        "bool": {
            "should": [
                {
                    "match": {
                        "Description": keyword
                    }
                }
            ]
        }
    }
    
})


# In[43]:


res['hits']['max_score']


# In[44]:


res['hits']['hits'][:4]


# In[ ]:




