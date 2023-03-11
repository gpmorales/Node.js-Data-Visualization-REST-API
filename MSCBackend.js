// BACKEND API architecture for handling the GET graphic requests on our AirEmory Website
const express = require('express');
const app = express();
const CSVParser = require('csv-parser');
const ReverseCSVParser = require('./ReverseFileParser.js');
const CSVWriter = require('fs');
const SSHClient = require('ssh2').Client;
const json2csv = require('json2csv').parse;
const { spawn } = require('child_process');
const moment = require('moment-timezone');

// Child processes that runs separate R-scripts to create graphics
const RWeekly = spawn('C:\\Program Files\\R\\R-4.2.1\\bin\\Rscript.exe', [ "./R_scripts/Weekly.R" ]);
const RDaily = spawn('C:\\Program Files\\R\\R-4.2.1\\bin\\Rscript.exe', [ "./R_scripts/Daily.R" ]);
const RMonthly = spawn('C:\\Program Files\\R\\R-4.2.1\\bin\\Rscript.exe', [ "./R_scripts/Monthly.R" ]);
const RQuarterYearly = spawn('C:\\Program Files\\R\\R-4.2.1\\bin\\Rscript.exe', [ "./R_scripts/QuarterYearly.R" ]);

//**** Middleware handling ****
const Router = express.Router();

// When you call app.use('/url...', router), you are telling Express to USE the ROUTER MIDDLEWARE for ALL ROUTES THAT START WITH THE PATH '/api/v1/tasks'
// This means that whenever a request comes in with a URL that starts with '/api/v1/tasks', Express will check the router to see if there is a matching route defined. 
app.use("/", Router);

//***Short Term graphics can be loaded unto same page
Router.route('/short-term').get(ShortTermGraphics);

//***Long Term graphics can be loaded into a different page ? TBD
Router.route('/long-term').get(LongTermGraphics);




//NOTE ********************* HERE WE HANDLE THE 'GET' CSV SHEETS REQUEST FOR SHORTTERM GRAPHICS ********************** NOTE
// ONE MAIN FUNCTION THAT CREATES WEEKLY AND DAILY GRAPHICS
async function ShortTermGraphics(request, response){
	try{  //Will return CSV with weekly data
		const DateParams = ShortTermParameters();
		let SplitMonths = true;
		let currMonth = true; //assume we are in the current month
		//IF REQUEST=> PM25.PM10 / Temp / Humidity TBD
		
		//If the week lies within the SAME month
		if(DateParams.length == 4){
			let SHORT_TERM_CSV_DF = await WeeklyGraphics(request, response, DateParams, !SplitMonths, currMonth);
			//DailyGraphics(SHORT_TERM_CSV_DF, DateParams[3]);
		}

		else{ //If the week is between 2 months and we have to concat to CSV objects
			const DateParams_Copy1 = [...DateParams.slice(0,1), ...DateParams.slice(2)]; //Previous Month
			const DateParams_Copy2 = [...DateParams.slice(1)]; //Current Month

			let SHORT_TERM_DF_1 = await WeeklyGraphics(request, response, DateParams_Copy1, SplitMonths, !currMonth); //data from Prev month -> We parse in reverse so its quicker
			let SHORT_TERM_DF_2 = await WeeklyGraphics(request, response, DateParams_Copy2, SplitMonths, currMonth); //data from Current Month
			let CONCAT_SHORT_TERM_DF = SHORT_TERM_DF_1.concat(SHORT_TERM_DF_2); // Dec 29, Dec 30, Dec 31 + Jan 1, Jan2, ... etc

			//Create Weekly Graphics here if data was concat from 2 separate CSV sheets
			const csv = json2csv(CONCAT_SHORT_TERM_DF);

			RWeekly.stdin.write(csv);
			RWeekly.stdin.end();

			RWeekly.on('close', (exit_code) => {
				console.log("Exited from R_Weekly script\n");
			});
			
		  DailyGraphics(CONCAT_SHORT_TERM_DF, DateParams_Copy1[3]);
		}

	}
	catch(err){
		console.log(err)
	}

} ShortTermGraphics();



//NOTE Return csvObject for graphic when we receive corresponding request for SHORT TERM graphics
async function WeeklyGraphics(request, response, DateParams, SplitMonths, isCurrentMonth){
	try{
		// Connect to Our Cluster and Stream through the data & parse CSV sheet here 
		let QueriedData = []; //our Data Frame essentially

		let PARSED_IN_REVERSED_ORDER = false; //Flag to check if data was parsed backwards for efficiency reasons

		const connection = new SSHClient(); //create Connection to SSH client

		//NOTE PROMISE WILL RETURN OUR CSV AFTER ALL OPERATIONS END ... we wait for the CSV data to be pushed into our Array before returning
		return new Promise((resolve,reject) => { 
			connection.on("ready", () => { //when connection is ready, perform Secure File Transfer Protocol

				console.log("SSH Client is ready");
				
				connection.sftp((error, SFTP) => {
					if(error) throw error;

					// Instantiate CSV READSTREAM object
					const CSVReadStream = SFTP.createReadStream(DateParams[0]); //params[0] is the file path for the CSV that holds all data

					//NOTE Instantiate CSV PARSER object -> the CSV Parser obj allows us to convert encoded data from our CSV ReadStream into text
					let parser = CSVParser({ columns : true });

					//If we are parsing the previous months csv (data spans over to last week of that month) OR its just one month and the start date is towards the end
					if( (SplitMonths && !isCurrentMonth) || (DateParams[1].getDate() >= 20 && !SplitMonths) ){ 
						parser = new ReverseCSVParser(['date', 'pm25', 'pm10', 'temperature', 'humditiy']);
						PARSED_IN_REVERSED_ORDER = true;
						console.log("REVERSE => " + DateParams[0]);
					} 
					
					//Process data from our READSTREAM obj and INSERT INTO A NEW CSV SHEET
					CSVReadStream.pipe(parser).on('data', (row) => { //createReadStream obj has pipe method that allows how to parse data chunks
						// create Datetime object from eash row's Timestamp

						const RowTimeStamp = new Date(row.date);  //time of each row (NOTE if UTC -> date - 5 * 60 * 60 * 1000)
						
						/*
						//Weekly CSV -> the data we query from the stream should be between 25 hours ago and 1 hour ago
						if(DateParams[1] <= RowTimeStamp && RowTimeStamp <= DateParams[2] && row.pm25 != ''){
							//ADD to JSON object
							if(!TIMESTAMP_FORMATTING(row.date)) row.date = FORMAT_CORRECTION(row.date);
							QueriedData.push(row);
						} 

						else if(RowTimeStamp > DateParams[2]){ //Destroy readstream once all data is fetched
							CSVReadStream.destroy();
						} */

						if(!TIMESTAMP_FORMATTING(row.date)) row.date = FORMAT_CORRECTION(row.date);
						QueriedData.push(row);

					}); // FINISH PARSING STREAM

					//HANDLING EVENTS WHILE PARSING STREAM
					CSVReadStream.on('close', (err) => {
						if(err) console.error(err)
						console.log("Finished Parsing Stream -> Weekly Data Fetched!");

						if(PARSED_IN_REVERSED_ORDER) QueriedData.reverse();

						//If all data is in one month, we create graphics and return
						if(!SplitMonths){
							// HERE we will create a CSV string object from our array of objects using the json2csv module
							const csv = json2csv(QueriedData);

							// NOTE Then use the child_process package to execute Chiara's R script which produces the Air Quality Graphics
							RWeekly.stdin.write(csv);
							RWeekly.stdin.end();

							RWeekly.on('close', (exit_code) => {  // Resolve our data for our Promise obj once we exit from R script
									console.log("Exited from R_Weekly script\n");
									resolve(QueriedData); // RETURN QUERIED DATA ***
							});
							
							// Close our SFTP and SSH clients after we query data and create graphics
							SFTP.end();
							connection.end();
						}

						//Otherwise we return part of necessary data we need and create the graphics in another function
						else{
							resolve(QueriedData);
							SFTP.end();
							connection.end();
						}
					}); //conn.on('close') 
					
					// Error handling with Stream
					CSVReadStream.on('error', (err) => {
						reject(err);
					});

				}); //Finish handling ReadStream closing

			}); //Connection.on('ready')


			//Creating Connection to cluster via SSH, using SSH2 clients
			connection.connect({
				host : "lab0z.mathcs.emory.edu",
				port : 22,
				username : "gpmoral",
				password : "gMC12345#$"
			});
			
			//ERROR Handling for SSH socket connection
			connection.on('error', (error) => console.error(error)); 
			
		}); //END OF PROMISE OBJECT

							process.exit(0);//****************************************************************************************************************************
	} //END OF TRY Block

	catch(err){
		console.log(err);
	} 
}



// TODO **** SIMPLY FILTER CSV AND CALL DAILY R SCRIPT !!!
function DailyGraphics(QueriedData, startDateDaily){ //in this method slice data to be one day in length and run another R file fore graphic
	// DAILY GRAPHICS
	console.log("Parsing Daily Data...\n");

	// Container for our CSV daily sheet we will create by parsing the weekly sheet
	let QueriedDailyData = [];

	const N = QueriedData.length - 1;
	// Parse CSV sheet backwards (sheet stores weekly data so we only need to stop once we accumulate a previous day's worth of data)
	for(let i = N; startDateDaily <= new Date(QueriedData[i].date); i--){
		QueriedDailyData.push(QueriedData[i]); // 12:00, 11:00, 10:00, ... etc
	}

	QueriedDailyData.reverse(); //We parsed data backwards so newer dates are pushed to the back...
	
	// Create a CSV string object from our array of objects using the json2csv module
	const DailyCsv = json2csv(QueriedDailyData);

	// NOTE Then use the child_process package to execute Chiara's R script which produces the Air Quality Graphics
	RDaily.stdin.write(DailyCsv);
	RDaily.stdin.end();

	RDaily.on('close', (exit_code) => {
		console.log("Exited from R_Daily script\n");
	});

	return;
} 









//NOTE ********************** HERE WE HANDLE THE 'GET' REQUEST for CSV SHEETS FOR LONGTERM GRAPHICS ********************* NOTE
// Return filepath(s) for graphic when we receive corresponding request for Long TERM graphics
async function LongTermGraphics(request, response){
	try{  //Will return CSV with weekly data
			const DateParams = LongTermParameters(); //get list of dates bounds and file path
		  let LONGTERM_CSV_DF = await QuarterYearlyGraphics(request, response, DateParams);
		  MonthlyGraphics(LONGTERM_CSV_DF, DateParams[3]);
	}
	catch(err){
		console.log(err)
	}

}// LongTermGraphics();



//NOTE Return csvObject for graphic when we receive corresponding request for SHORT TERM graphics
async function QuarterYearlyGraphics(request, response, DateParams){
	try{
		// Connect to Our Cluster and Stream through the data & parse CSV sheet here 
		let csvSheets = DateParams[0]; //csvSheet file names

		let QueriedData = []; //our Data Frame essentially

		const connection = new SSHClient(); //create Connection to SSH client

		//NOTE PROMISE WILL RETURN OUR CSV AFTER ALL OPERATIONS END ... we wait for the CSV data to be pushed into our Array before returning
		for(let i = 0; i < csvSheets.length ; i++){
			QueriedData = []; //To prevent data from being re-copied as the async operations occur out of order
			
			// reinstantiate the Container as we add the sheets to our spreadsheet
			await new Promise((resolve, reject) => {

				connection.on("ready", () => { //when connection is ready, perform Secure File Transfer Protocol
					//console.log("SSH Client is ready!\n");
					connection.sftp((error, SFTP) => {
						if(error) throw error;

						const CSVReadStream = SFTP.createReadStream(csvSheets[i]); //CSV is the file path for the CSV that holds all data
						const parser = CSVParser({ columns : true });

						//PARSE DATA from our READ STREAM and INSERT INTO A NEW CSV SHEET
						CSVReadStream.pipe(parser).on('data', (row) => { //createReadStream obj has pipe method that allows how to parse data chunks
							// create Datetime object from eash row's Timestamp
							const RowTimeStamp = new Date(row.date); //time of each row

							//TriYearly CSV -> the data we query from the stream should be between 120 days ago and 1 day ago
							if(DateParams[1] <= RowTimeStamp && RowTimeStamp <= DateParams[2] && row.pm25 != '' ){
								//ADD to new CSV
								if(!TIMESTAMP_FORMATTING(row.date)) row.date = FORMAT_CORRECTION(row.date);
								QueriedData.push(row);
							} 

							else if(RowTimeStamp > DateParams[2]){ //Destory ReadStream once bounded data is fetched
								CSVReadStream.destroy();
							} 

						}); // FINISH PARSING STREAM

						//HANDLING EVENTS WHILE PARSING STREAM
						CSVReadStream.on('close', (err) => {
							if(err) console.error(err)
							resolve(QueriedData);
						});
						
						// Error handling with Stream
						CSVReadStream.on('error', (err) => {
							reject(err);
						});

					}); //Finish handling ReadStream closing

				}); //Finish connection

				//Creating Connection to cluster via SSH, using SSH2 clients
				connection.connect({
					host : "lab0z.mathcs.emory.edu",
					port : 22,
					username : "gpmoral",
					password : "gMC12345#$"
				});

			}); //END OF PROMISE

		} //END OF FOR-LOOP

		// CLOSE OUR CLIENTS/CONNECTION
		connection.end();
		console.log("Finished Parsing CSV Sheets -> All Quarter-Yearly Data Fetched (" + QueriedData.length + " Data Points");

		// NOTE here we will create a CSV string object from our array of objects using the json2csv module
		QueriedData.sort((a,b) => new Date(a.date) - new Date(b.date));

		const csv = json2csv(QueriedData);

		// NOTE Then use the child_process package to execute Chiara's R script which produces the Air Quality Graphics
		RQuarterYearly.stdin.write(csv);
		RQuarterYearly.stdin.end();

		RQuarterYearly.on('close', (exit_code) => {
			console.log("\nExited from R_Quarter_Yearly script");
		}); 

		//NOTE RETURN CSV DATAFRAME FOR MONTHLY (WE JUST FILTER THE DATA AGAIN in a Synchronous manner)
		return QueriedData;
	}

	catch(err){
		console.log(err);
	}
}


// TODO **** SIMPLY FILTER CSV AND CALL DAILY R SCRIPT !!!
function MonthlyGraphics(QueriedData, startDateMonthly){ //in this method slice data to be one day in length and run another R file fore graphic
	// DAILY GRAPHICS
	console.log("\nParsing Monthly Data...\n");

	// Container for our CSV daily sheet we will create by parsing the weekly sheet
	let QueriedMonthlyData = [];

	const N = QueriedData.length - 1;
	// Parse CSV sheet backwards (sheet stores weekly data so we only need to stop once we accumulate a previous day's worth of data)
	for(let i = N; startDateMonthly <= new Date(QueriedData[i].date) ; i--){
		QueriedMonthlyData.push(QueriedData[i]); // 12:00, 11:00, 10:00, ... etc
	}

	QueriedMonthlyData.reverse(); //We parsed data backwards so newer dates are pushed to the back...

	// Create a CSV string object from our array of objects using the json2csv module
	const MonthlyCsv = json2csv(QueriedMonthlyData);

	// NOTE Then use the child_process package to execute Chiara's R script which produces the Air Quality Graphics
	RMonthly.stdin.write(MonthlyCsv);
	RMonthly.stdin.end();

	RMonthly.on('close', (exit_code) => {
		console.log("Exited from R_Monthly script");
	});

	return;
} 





// NOTE HANDLE ALL FILE PATHS AND DATE TIME BOUNDING HERE NOTE //
function LongTermParameters(){
	// We will create daily and weekly graphics by parsing and selecting data btwn these bounds from SSH CSV
	const temp = new Date();
	let offSet = -5;
	const currTime = new Date(temp.getTime() + (offSet * 60 * 60 * 1000));

	//Month Before -> start date will be 30 days earlier at the previous hour (ex: Sunday 3:23 pm -> 3:
	const startDateMonthly = new Date(currTime.getTime() - (31 * 24 * 60 * 60 * 1000)); 

	//4 Months -> end a day before & being 4 months + 1 day previously
	const startTriYearlyDate = new Date(currTime.getTime() - (91 * 24 * 60 * 60 * 1000)); 
	const endTriYearlyDate = new Date(currTime.getTime() - ( 1 * 24 * 60 * 60 * 1000 ));

	const date = new Date();

	let csv_Year = date.getFullYear();

	let months = ["Jan", "Feb", "Mar", "April", "May", "June", "July", "August", "Sep", "Oct", "Nov", "Dec"]

	//Store all CSV sheets
	let csvSheets = []; 

	let currMonth = date.getMonth();

	// NOTE Circular indexing
	// Grab CSV sheets for the current Month and 4 preceeding months
	let Month_csv1, Month_csv2, Month_csv3, Month_csv4, Month_csv5;

	if(currMonth >= 3){
		// Ex -> curMonth = May -> index 
		Month_csv1 = months[currMonth] + csv_Year; //Apr 14
		Month_csv2 = months[currMonth - 1] + csv_Year; // Mar 14 (1 month)
		Month_csv3 = months[currMonth - 2] + csv_Year; // Feb 14 (2 months)
		Month_csv4 = months[currMonth - 3] + csv_Year; // Jan 14 (3 months)
	}

	else if(currMonth == 2){
		Month_csv1 = months[currMonth] + csv_Year; //Mar
		Month_csv2 = months[currMonth - 1] + csv_Year; //Feb
		Month_csv3 = months[currMonth - 2] + csv_Year; //Jan
		Month_csv4 = months[11] + (csv_Year - 1); //Dec
	}

	else if(currMonth == 1){
		Month_csv1 = months[currMonth] + csv_Year; //Feb
		Month_csv2 = months[currMonth - 1] + csv_Year; //Jan
		Month_csv3 = months[11] + (csv_Year - 1); //Dec
		Month_csv4 = months[10] + (csv_Year - 1); //Nov
	}

	else if(currMonth == 0){
		Month_csv1 = months[currMonth] + csv_Year; //Jan
		Month_csv2 = months[11] + (csv_Year - 1); //Dec
		Month_csv3 = months[10] + (csv_Year - 1); //Nov
		Month_csv4 = months[9] + (csv_Year - 1); //Oct
	}

	/*
	csvSheets.push(Month_csv4);
	csvSheets.push(Month_csv3);
	csvSheets.push(Month_csv2);
	csvSheets.push(Month_csv1);

	return [csvSheets, startTriYearlyDate, endTriYearlyDate, startDateMonthly];
	*/

	csvSheets.push("/home/gpmoral/Public/Dec21.csv");
	csvSheets.push("/home/gpmoral/Public/Jan22.csv");
	csvSheets.push("/home/gpmoral/Public/Feb22.csv");
	csvSheets.push("/home/gpmoral/Public/Mar22.csv");

	return [csvSheets, new Date(new Date(2021,11,30,0,0,0) - 5 * 60 * 60 * 1000),
										 new Date(new Date(2022,2,31,0,0,0) - 5 * 60 * 60 * 1000), 
										 new Date(new Date(2022,1,27,0,0,0) - 5 * 60 * 60 * 1000) //startDateMonthly
	];
} 


///////////////////////////////
function ShortTermParameters(){
	// We will create daily and weekly graphics by parsing and selecting data btwn these bounds from SSH CSV
	const temp = new Date();
	let offSet = -5;
	const currTime = new Date(temp.getTime() + (offSet * 60 * 60 * 1000));

	//week Before + 1 hour before -> start date will be 7 days earlier at the hour (ex: Sunday 3:23 pm -> 3:
	const startDateWeekly = new Date(currTime.getTime() - (7 * 24 * 60 * 60 * 1000) -  (1 * 60 * 60 * 1000)); 
	const endDateWeekly = new Date(currTime.getTime() - (1 * 60 * 60 * 1000)); 

	//End 1 hour before -> start date will be 25 hours earlier (ex: Sunday 3:23 pm -> Sat 2:23 - Sun 2:23
	const startDateDaily = new Date(currTime.getTime() - (25 * 60 * 60 * 1000)); 

	const date = new Date();
	var csv_path = date.getFullYear(); //For our CSV

	const months = ["Jan", "Feb", "Mar", "April", "May", "June", "July", "August", "Sep", "Oct", "Nov", "Dec"]

	/*
	//Create File Paths
	if(date.getDate() >= 7) csv_path = months[date.getMonth()] + csv_path;

	else if(date.getDate() < 6 && date.getMonth() > 0){ //if we are within the first week of the month
		let csv_path1 = months[date.getMonth() - 1] + csv_path; //prev month
		let csv_path2 = months[date.getMonth()] + csv_path; //curr month
		csv_path1 = "/home/gpmoral/" + csv_path1 + ".csv";
		csv_path2 = "/home/gpmoral/" + csv_path2 + ".csv";
		return [csv_path1, csv_path2, startDateWeekly, endDateWeekly, startDateDaily];
	}

	else if(date.getDate() < 6 && date.getMonth() == 0){ //if we are within the first week of the month AND its January 
		let csv_path1 = months[11] + (csv_path - 1); //Dec of prev year
		let csv_path2 = months[date.getMonth()] + csv_path; //Jan of cur yr
		csv_path1 = "/home/gpmoral/" + csv_path1 + ".csv";
		csv_path2 = "/home/gpmoral/" + csv_path2 + ".csv";
		return [csv_path1, csv_path2, startDateWeekly, endDateWeekly, startDateDaily];
	}
	
	const CSVPath = "/home/gpmoral/" + csv_path + ".csv";
	
	return [CSVPath, startDateWeekly, endDateWeekly, startDateDaily];
	*/
	
	return ["/home/gpmoral/Music/Mar2023.csv", new Date(2022,0,14,0,0,0), new Date(2022,0,21,0,0,0), new Date(2022,0,20,0,0,0)];
} 



///////// HELPER FUNCTIONS ///////////
function TIMESTAMP_FORMATTING(TimeStamp){
	const regex = /^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}$/;
	if (regex.test(TimeStamp)) return true;
	return false;
}

// Corrects the formating of the time stamp to match MM/DD/YYYY HH:mm
function FORMAT_CORRECTION(TimeStamp){
	const dateParts = TimeStamp.substring(0, 10).split('-');
	const timeParts = TimeStamp.substring(11, 16).split(':');

	// Create a new date string in the desired format
	const newDateString = `${dateParts[1]}/${dateParts[2]}/${dateParts[0]} ${timeParts[0]}:${timeParts[1]}`;

	if(TIMESTAMP_FORMATTING(newDateString))
		return newDateString;
} 
//END OF FILE
