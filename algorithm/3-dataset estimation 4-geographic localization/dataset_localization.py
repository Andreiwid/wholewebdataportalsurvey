import time
import datetime
import urllib.request
import ssl
import json
import pandas as pd
from pandas.io.json import json_normalize

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

#input file
urls = pd.read_csv(r'dataportals_identified_run4.csv', delimiter=';', encoding='latin-1')

#output files
file_results = open(r'results.csv',encoding='utf-8',mode='w') 
file_results.write('Start: ' + str(datetime.datetime.now()) + '\n')
file_results.write("---\n")
file_results.write('TIMESTAMP;DEPTH;ID;URL;DOMAIN;PRODUCT;DATASET_TOTAL;DOMAIN_COUNTRY;IP_COUNTRY;ROOT_DOMAIN\n')
file_errors = open(r'errors.csv',encoding='utf-8',mode='w') 
file_errors.write("Start: " + str(datetime.datetime.now()) + "\n")
file_errors.write("---\n")
file_errors.write('TIMESTAMP;DEPTH;ID;URL;DOMAIN;PRODUCT;ERROR_MESSAGE\n')

print('records: ' + str(len(urls)))
print('start: ' + str(datetime.datetime.now()))

for x, row_url in urls.iterrows():
    #get the root domain to identify duplicates across depths #0, #1 and #2 later on
    #root domain does not have http://, https:// and www. Nor includes paths
    url = row_url['URL']
    if url.find('www.') != -1:
        url = url[url.find('www.') + 4:]
    else:
        url = url[url.find('//') + 2:]
    if url.find('/') != -1:
        root_domain = url[:url.find('/')]
    else:
        root_domain = url 
    
    #get domain and IP country
    #==========================
    ip_country = ''
    url = row_url['URL']
    url = url[url.find('//') + 2:]
    if url.find('/') != -1:
        domain = url[:url.find('/')]
    else:
        domain = url 
    req = urllib.request.Request(
        'http://ip-api.com/json/' + domain, 
        data=None, 
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
        }
    )
    resp = urllib.request.urlopen(req, timeout=20, context=ctx)
    response_dict = json.loads(resp.read())
    if response_dict['status'] == 'success':
        ip_country = response_dict['country']
    else:
        ip_country = '(not available)'
    
    #get the Country Code Top Level Domain
    domain_country = ''
    url = row_url['URL']
    url = url[url.find('//') + 2:]
    if url.find('/') != -1:
        url = url[:url.find('/')]
        cctld = url[url.rfind('.') + 1:]
    else:
        cctld = url[url.rfind('.') + 1:]
    cctld = domain[domain.rfind('.') + 1:]
    if len(cctld) == 2:
        req = urllib.request.Request(
            'https://restcountries.eu/rest/v2/alpha/' + cctld, 
            data=None, 
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
            }
        )
        try:
            resp = urllib.request.urlopen(req, timeout=20, context=ctx)
            response_dict = json.loads(resp.read())

        except Exception as e:
            domain_country = '(not available)'

        else:
            domain_country = response_dict['name']
    else:
        domain_country = '(not available)'
    
    #CKAN ==================================================================================================================
    if row_url['PRODUCT'] == 'CKAN':
        try:
            url = row_url['URL'] + '/api/action/package_search?rows=1'

            req = urllib.request.Request(
                url, 
                data=None, 
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
                }
            )
            resp = urllib.request.urlopen(req, timeout=60, context=ctx)
        
        except Exception as e: #raises HTTP, URL and refused connection errors
            print(str(row_url['ID']) + ' ' + url + ' CONNECTION ISSUE\n')
            file_errors.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' +  str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';CKAN;' + str(e) + '\n')

        else:
            #parse JSON
            try:
                response_dict = json.loads(resp.read())
                CKAN_datasetcount = int(response_dict['result']['count'])

            except (ValueError, TypeError, KeyError) as e:
                print(str(row_url['ID']) + ' ' + url + ' PRODUCT:ERROR/UNKNOWN\n')
                file_errors.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' +  str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';CKAN;' + str(e) + '\n')

            else:
                print(str(row_url['ID']) + ' ' + url + ' ' + str(CKAN_datasetcount) + '\n')
                file_results.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' +  str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';CKAN;' + str(CKAN_datasetcount) + ';' + domain_country + ';' + ip_country + ';' + root_domain +'\n')
        
    #SOCRATA ==================================================================================================================
    if row_url['PRODUCT'] == 'SOCRATA':
        try:
            url = row_url['URL']
            domain = url[url.find('//') + 2:]

            #first build parameters with URL - if it returns any connection error, give another try with domain instead - see Exception
            url = url + '/api/catalog/v1/domains?only=dataset&domains=' + domain + '&search_context=' + domain

            req = urllib.request.Request(
                url, 
                data=None, 
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
                }
            )
            resp = urllib.request.urlopen(req, timeout=60, context=ctx)

        except Exception as e: #raises HTTP, URL and refused connection errors OR Socrata due to domain/search_context

            try:
                url = row_url['URL']
                domain = row_url['DOMAIN']
                domain = domain[domain.find('//') + 2:]

                #checks existence of port ":" ou path "/"
                #some Socrata consider domain instead of URL
                if domain.find(':') != -1:
                    domain = domain[:domain.find(':')]
                elif domain.find('/') != -1 :
                    domain = domain[:domain.find('/')]

                url = url + '/api/catalog/v1/domains?only=dataset&domains=' + domain + '&search_context=' + domain

                req = urllib.request.Request(
                    url, 
                    data=None, 
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
                    }
                )
                resp = urllib.request.urlopen(req, timeout=60, context=ctx)
        
            except Exception as e: #now sure it raises HTTP, URL and refused connection errors
                print(str(row_url['ID']) + ' ' + url + ' CONNECTION ISSUE\n')
                file_errors.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' +  str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';SOCRATA;' + str(e) + '\n')
                continue #next interation to avoid finally block

        #here we realied on finally due to double try to request URL with parameters options 1-URL and then 2-DOMAIN
        finally:
            #parse JSON
            try:
                response_dict = json.loads(resp.read())
                
                #we found some resultSetSize returned 0 - maybe it is a private catalog with login/password
                if int(response_dict['resultSetSize']) == 0:
                    SOCRATA_datasetcount = -1
                else:
                    SOCRATA_datasetcount = int(json_normalize(response_dict['results'])['count'])
                    
            except (ValueError, TypeError, KeyError) as e:
                print(str(row_url['ID']) + ' ' + url + ' PRODUCT:ERROR/UNKNOWN\n')
                file_errors.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' + str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';SOCRATA;' + str(e) + '\n')

            else:
                print(str(row_url['ID']) + ' ' + url + ' ' + str(SOCRATA_datasetcount) + '\n')
                file_results.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' + str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';SOCRATA;' + str(SOCRATA_datasetcount) + ';' + domain_country + ';' + ip_country + ';' + root_domain + '\n')

                
    #OPENDATASOFT ==================================================================================================================
    if row_url['PRODUCT'] == 'OPENDATASOFT':
        try:
            url = row_url['URL'] + '/api/v2/catalog/datasets?rows=1'

            req = urllib.request.Request(
                url, 
                data=None, 
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
                }
            )
            resp = urllib.request.urlopen(req, timeout=60, context=ctx)
        
        except Exception as e: #raises HTTP, URL and refused connection errors
            print(str(row_url['ID']) + ' ' + url + ' CONNECTION ISSUE\n')
            file_errors.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' +  str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';OPENDATASOFT;' + str(e) + '\n')

        else:
            #parse JSON
            try:
                response_dict = json.loads(resp.read())
                OPENDATASOFT_datasetcount = int(response_dict['total_count'])

            except (ValueError, TypeError, KeyError) as e:
                print(str(row_url['ID']) + ' ' + url + ' PRODUCT:ERROR/UNKNOWN\n')
                file_errors.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' +  str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';OPENDATASOFT;' + str(e) + '\n')

            else:
                print(str(row_url['ID']) + ' ' + url + ' ' + str(OPENDATASOFT_datasetcount) + '\n')
                file_results.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' +  str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';OPENDATASOFT;' + str(OPENDATASOFT_datasetcount) + ';' + domain_country + ';' + ip_country + ';' + root_domain + '\n')

                
    #ARCGIS ==================================================================================================================
    if row_url['PRODUCT'] == 'ARCGIS':
        try:
            url = row_url['URL'] + '/data.json?page=' + str(next_page_to_request) + '&per_page=' + str(pages_per_request)

            req = urllib.request.Request(
                url, 
                data=None, 
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
                }
            )
            resp = urllib.request.urlopen(req, timeout=20, context=ctx)

        except Exception as e: #raises HTTP, URL and refused connection errors
            print('ERRO: ' + url + ' ' + str(e))
            file_errors.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' +  str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';ARCGIS;' + str(e) + '\n')

        else:
            try:
                response_dict = json.loads(resp.read())
                dataset_info = json_normalize(response_dict['dataset'])

            except Exception as e:
                print(str(row_url['ID']) + ' ' + url + ' PRODUCT:ERROR/UNKNOWN\n')
                file_errors.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' +  str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';ARCGIS;' + str(e) + '\n')
                continue

            else:
                #just in case data.json returned no dataset - e.g.: https://data-brookhavenga.opendata.arcgis.com or https://data-eastlongmeadow.opendata.arcgis.com
                if dataset_info.empty:
                    print(str(row_url['ID']) + ' ' + url + ' PRODUCT:NO DATASET RETURN FROM DATA.JSON\n')
                    ARCGIS_datasetcount = -1
                    print(str(row_url['ID']) + ' ' + url + ' ' + str(ARCGIS_datasetcount) + '\n')
                    file_results.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' +  str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';ARCGIS;' + str(ARCGIS_datasetcount) + ';' + domain_country + ';' + ip_country + ';' + root_domain + '\n')
                    continue
                    
                #gets the first dataset identifier
                first_dataset_identifier = str(dataset_info['identifier'][0])
                first_dataset_identifier = first_dataset_identifier[first_dataset_identifier.find('/datasets/') + 10:]

                #gets installation Owner
                url = row_url['URL'] + '/api/v2/datasets/' + first_dataset_identifier

                req = urllib.request.Request(
                    url, 
                    data=None, 
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
                    }
                )
                resp = urllib.request.urlopen(req, timeout=20, context=ctx)
                response_dict = json.loads(resp.read())

                installation_owner = response_dict['data']['attributes']['owner']

                #gets dataset stats filtering Owner field
                url = row_url['URL'] + '/api/v2/datasets?filter[owner]=' + installation_owner + '&page[size]=1'

                req = urllib.request.Request(
                    url, 
                    data=None, 
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
                    }
                )
                resp = urllib.request.urlopen(req, timeout=20, context=ctx)
                response_dict = json.loads(resp.read())

                ARCGIS_datasetcount = int(response_dict['meta']['stats']['totalCount'])

                #print(str(row_url['URL']) + ' first dataset: ' + first_dataset_identifier + ' owner: ' + installation_owner + ' count: ' + str(ARCGIS_datasetcount))

                print(str(row_url['ID']) + ' ' + url + ' ' + str(ARCGIS_datasetcount) + '\n')
                file_results.write(str(datetime.datetime.now()) + ';' + str(row_url['DEPTH']) + ';' + str(row_url['ID']) + ';' +  str(row_url['URL']) + ';' + str(row_url['DOMAIN']) + ';ARCGIS;' + str(ARCGIS_datasetcount) + ';' + domain_country + ';' + ip_country + ';' + root_domain + '\n')
                
file_results.write("---\n")
file_results.write('End: ' + str(datetime.datetime.now()))
file_errors.write("---\n")
file_errors.write('End: ' + str(datetime.datetime.now()))

file_results.close()
file_errors.close()

print('end: ' + str(datetime.datetime.now()))