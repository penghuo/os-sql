# Security Settings  

## Introduction  

User needs `cluster:admin/opensearch/ppl` permission to submit synchronous or asynchronous PPL queries. Polling and deleting retained asynchronous jobs are independently authorized with `cluster:admin/opensearch/ppl/async_query/result` and `cluster:admin/opensearch/ppl/async_query/delete`. User also needs indices level permission `indices:admin/mappings/get` to get field mappings, `indices:monitor/settings/get` to get cluster settings, and `indices:data/read/search*` to search index.

Every asynchronous GET and DELETE request is reauthorized. Knowing a job ID does not grant access to its metadata or result. The caller must have the operation permission, match the submitting user and tenant, and retain all backend roles captured at submission.
## Using Rest API  

**--INTRODUCED 2.1--**  

Example: Create the ppl_role for test_user. then test_user could use PPL to query `ppl-security-demo` index.  
1. Create the ppl_role and grant permission to access PPL plugin and access ppl-security-demo index  
  
```bash
PUT _plugins/_security/api/roles/ppl_role
{
  "cluster_permissions": [
    "cluster:admin/opensearch/ppl",
    "cluster:admin/opensearch/ppl/async_query/result",
    "cluster:admin/opensearch/ppl/async_query/delete"
  ],
  "index_permissions": [{
    "index_patterns": [
      "ppl-security-demo"
    ],
    "allowed_actions": [
      "indices:data/read/search*",
      "indices:admin/mappings/get",
      "indices:monitor/settings/get"
    ]
  }]
}

```
  
2. Mapping the test_user to the ppl_role  
  
```bash
PUT _plugins/_security/api/rolesmapping/ppl_role
{
  "backend_roles" : [],
  "hosts" : [],
  "users" : ["test_user"]
}


```
  
## Using Security Dashboard  

**--INTRODUCED 2.1--**  

Example: Create ppl_access permission and add to existing role  
1. Create the ppl_access permission  
  
```bash
PUT _plugins/_security/api/actiongroups/ppl_access
{
  "allowed_actions": [
    "cluster:admin/opensearch/ppl",
    "cluster:admin/opensearch/ppl/async_query/result",
    "cluster:admin/opensearch/ppl/async_query/delete"
  ]
}

```
  
2. Grant the ppl_access permission to ppl_test_role  
  
![Image](https://user-images.githubusercontent.com/2969395/185448976-6c0aed6b-7540-4b99-92c3-362da8ae3763.png)
