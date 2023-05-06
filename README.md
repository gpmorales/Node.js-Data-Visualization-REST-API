# Node.js-Data-Visualization-REST-API-with-R-Integration-via-SSH
This is a Node.js server-side program that handles requests for short-term and long-term graphics of air-quality data stored in CSV files. 
It uses several Node.js packages and external scripts. 
The code handles GET requests for air-quality graphics and data stored at the servers in our Math and Science Center.
These libraries allow the code to perform various tasks, such as parsing CSV files, creating SSH connections, and spawning child processes to run R scripts. 
The express library is used to create a web server and handle incoming HTTP requests, while csv-parser is used to parse CSV files and convert them into JSON objects. 
The fs library is used to read and write files, while ssh2 is used to create a secure connection to a remote server. 
The json2csv library is used to convert JSON data into CSV format, which can be used to generate charts and graphs using R scripts. 
Finally, child_process is used to spawn child processes to run R scripts and moment-timezone is used to work with dates and times.
