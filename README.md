# gctools-report
Collection of scripts and instructions to grab data!

Note: This repo assumes you have kubectl set up for the CollabAKS kubernetes cluster.

To get the monthly report, run
```
python report.py year month
```
where year is an integer and month is an integer from 1-12.

The resulting reports can be found in the folder 'reports'.


## Additional notes

The docker version works provided a kubectl binary is made available at /usr/local/bin/kubectl  along with the config file in ~/.kube/config which has the relevant k8s clusters registered

If you don't have access to the kubernetes clusters set up, the easiest way would be using:

`az aks get-credentials -g <name of the resource group that contains the aks cluster> -n <cluster name>`

and then make sure that the names the clusters are registered under in your config match the --context parameter in kube_connect.py


Ensure you have access to the collab db server on the mysql port (3306)

### Windows notes

python should be added to the PATH variable to be able to run the scripts from command line

kubectl must be added to PATH or be in some other way accessible from the terminal to allow the scripts to run kubectl port-forward commands

## Architecture

monthly_report.py and quarterly_report.py are the main files, which use the others to gather all of the data of each tool. 

Generally, each tool has three files: the configuration file, a connection file, the querying file, and a report generator file. The config file contains the information to connect to the relevant database/API. The connection file uses the configuration file to form the connection. The querying file uses the connection file to access and query the database. Finally, the report generator file uses the querying file to customize the queries and store the results.

For collab and Helpdesk, the connection file and querying file are in one.

For Connex and Pedia, we don't have access, and so instead it creates the queries needed, but doesn't run them.
