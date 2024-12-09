'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const mustache = require('mustache');
const filesystem = require('fs');
const url = new URL(process.argv[3]);
const hbase = require('hbase');
require('dotenv').config()
const port = Number(process.argv[2]);

var hclient = hbase({
	host: url.hostname,
	path: url.pathname ?? "/",
	port: url.port || (url.protocol === 'http:' ? 80 : 443),
	protocol: url.protocol.slice(0, -1), // Don't want the colon
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});


const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));

app.get('/test-hbase', async function (req, res) {
	const tableName = "shichen_taxi_trips_by_company";
	if (!tableName) {
		res.status(400).send("Table name is required");
		return;
	}

	hclient.table(tableName)
		.scan({}, function (err, rows) {
			if (err) {
				console.error(`Error querying table ${tableName}:`, err);
				res.status(500).send(`Error querying table ${tableName}: ${err}`);
				return;
			}

			console.log(`Successfully queried table ${tableName}`);
			console.log(rows);

			// Return a subset of the data for verification
			res.send({
				message: `Successfully queried table ${tableName}`,
				sample: rows.slice(0, 10) // Return only the first 10 rows
			});
		});
});


app.get('/company-stats', async function (req, res) {
	const company = req.query['company'];
	if (!company) {
		res.status(400).send("Company name is required");
		return;
	}

	console.log("Requested company:", company);

	// Helper function to query batch layer
	function queryBatchLayer(companyName, callback) {
		hclient.table('shichen_taxi_trips_by_company')
			.row(companyName)
			.get(function (err, cells) {
				if (err) {
					console.error(`Error querying batch layer:`, err);
					callback(err, null);
					return;
				}

				// Parse batch layer data
				const stats = {};
				cells.forEach(cell => {
					const column = cell.column.split(':')[1]; // Extract the column name
					stats[column] = cell['$']; // Store the value
				});
				callback(null, stats);
			});
	}

	// Helper function to query speed layer
	function querySpeedLayer(companyName, callback) {
		hclient.table('shichen_taxi_trips_speed')
			.scan(
				{
					filter: {
						"type": "SingleColumnValueFilter",
						"family": Buffer.from('trip_info').toString('base64'),
						"qualifier": Buffer.from('company').toString('base64'),
						"op": "EQUAL",
						"comparator": {
							"type": "BinaryComparator",
							"value": Buffer.from(companyName).toString('base64')
						}
					}
				},
				function (err, rows) {
					if (err) {
						console.error(`Error querying speed layer:`, err);
						callback(err, null);
						return;
					}

					// Aggregate speed layer data
					let totalFare = 0;
					let totalTripSeconds = 0;
					let tripCount = 0;
					const pickupAreas = {};
					const dropoffAreas = {};

					rows.forEach(row => {
						const fare = parseFloat(row.column === 'trip_info:fare' ? row['$'] : 0);
						const tripSeconds = parseInt(row.column === 'trip_info:trip_seconds' ? row['$'] : 0);
						const pickupArea = row.column === 'trip_info:pickup_area' ? row['$'] : null;
						const dropoffArea = row.column === 'trip_info:dropoff_area' ? row['$'] : null;

						totalFare += fare;
						totalTripSeconds += tripSeconds;
						tripCount++;

						// Track pickup/dropoff area frequency
						if (pickupArea) pickupAreas[pickupArea] = (pickupAreas[pickupArea] || 0) + 1;
						if (dropoffArea) dropoffAreas[dropoffArea] = (dropoffAreas[dropoffArea] || 0) + 1;
					});

					// Find most common pickup/dropoff areas
					const topPickupArea = Object.keys(pickupAreas).reduce((a, b) => pickupAreas[a] > pickupAreas[b] ? a : b, "N/A");
					const topDropoffArea = Object.keys(dropoffAreas).reduce((a, b) => dropoffAreas[a] > dropoffAreas[b] ? a : b, "N/A");

					callback(null, { totalFare, totalTripSeconds, tripCount, topPickupArea, topDropoffArea });
				}
			);
	}

	// Query batch layer
	queryBatchLayer(company, (err, batchLayerData) => {
		if (err) {
			res.status(500).send("Batch Layer query failed");
			return;
		}

		// Query speed layer
		querySpeedLayer(company, (err, speedLayerData) => {
			if (err) {
				res.status(500).send("Speed Layer query failed");
				return;
			}

			// Merge results
			const totalFare = (parseFloat(batchLayerData['total_fare'] || 0) + (speedLayerData?.totalFare || 0)).toFixed(2);
			const totalTripSeconds = (parseFloat(batchLayerData['total_trip_seconds'] || 0) + (speedLayerData?.totalTripSeconds || 0)).toFixed(2);
			const tripCount = (parseInt(batchLayerData['trip_count'] || 0, 10) + (speedLayerData?.tripCount || 0));

// 计算平均值，确保只使用有意义的数据
			let avgFare;
			if (tripCount > 0) {
				avgFare = (parseFloat(totalFare) / tripCount).toFixed(2);
			} else if (batchLayerData['total_fare']) {
				avgFare = (parseFloat(batchLayerData['total_fare']) / parseInt(batchLayerData['trip_count'], 10)).toFixed(2);
			} else if (speedLayerData?.totalFare) {
				avgFare = (speedLayerData.totalFare / speedLayerData.tripCount).toFixed(2);
			} else {
				avgFare = "0.00"; // 当两层均无数据时，默认为 0.00
			}

			let avgTripSeconds;
			if (tripCount > 0) {
				avgTripSeconds = (parseFloat(totalTripSeconds) / tripCount).toFixed(2);
			} else if (batchLayerData['total_trip_seconds']) {
				avgTripSeconds = (parseFloat(batchLayerData['total_trip_seconds']) / parseInt(batchLayerData['trip_count'], 10)).toFixed(2);
			} else if (speedLayerData?.totalTripSeconds) {
				avgTripSeconds = (speedLayerData.totalTripSeconds / speedLayerData.tripCount).toFixed(2);
			} else {
				avgTripSeconds = "0.00"; // 当两层均无数据时，默认为 0.00
			}

			// 获取 Pickup 和 Dropoff 信息，优先返回有数据的层
			const topPickupArea = batchLayerData['top_pickup_area'] && speedLayerData?.topPickupArea
				? `${batchLayerData['top_pickup_area']}`
				: batchLayerData['top_pickup_area'] || speedLayerData?.topPickupArea || "Unknown";

			const topDropoffArea = batchLayerData['top_dropoff_area'] && speedLayerData?.topDropoffArea
				? `${batchLayerData['top_dropoff_area']}`
				: batchLayerData['top_dropoff_area'] || speedLayerData?.topDropoffArea || "Unknown";

			const responseData = {
				company: company,
				tripCount: tripCount,
				totalFare: totalFare,
				totalTripSeconds: totalTripSeconds,
				averageFare: avgFare,
				averageTripSeconds: avgTripSeconds,
				topPickupArea: topPickupArea,
				topDropoffArea: topDropoffArea
			};

			// Render result.mustache
			const template = filesystem.readFileSync('result.mustache').toString();
			const renderedHtml = mustache.render(template, responseData);

			res.send(renderedHtml);
		});
	});
});


// app.get('/company-stats', async function (req, res) {
// 	const company = req.query['company'];
// 	if (!company) {
// 		res.status(400).send("Company name is required");
// 		return;
// 	}
//
// 	// Helper function to query batch layer
// 	function queryBatchLayer(companyName, callback) {
// 		hclient.table('shichen_taxi_trips_by_company')
// 			.row(companyName)
// 			.get(function (err, cells) {
// 				if (err) {
// 					console.error(`Error querying batch layer:`, err);
// 					callback(err, null);
// 					return;
// 				}
//
// 				// Parse batch layer data
// 				const stats = {};
// 				cells.forEach(cell => {
// 					const column = cell.column.split(':')[1]; // Extract the column name
// 					stats[column] = cell['$']; // Store the value
// 				});
// 				callback(null, stats);
// 			});
// 	}
//
// 	// Query batch layer
// 	queryBatchLayer(company, (err, batchLayerData) => {
// 		if (err) {
// 			res.status(500).send("Batch Layer query failed");
// 			return;
// 		}
//
// 		// Parse batch layer data
// 		const tripCount = parseInt(batchLayerData['trip_count'] || 0, 10);
// 		const totalFare = parseFloat(batchLayerData['total_fare'] || 0);
// 		const totalTripSeconds = parseFloat(batchLayerData['total_trip_seconds'] || 0);
// 		const topPickupArea = batchLayerData['top_pickup_area'] || "N/A";
// 		const topDropoffArea = batchLayerData['top_dropoff_area'] || "N/A";
//
// 		// Calculate averages
// 		const avgFare = tripCount > 0 ? (totalFare / tripCount).toFixed(2) : "N/A";
// 		const avgTripSeconds = tripCount > 0 ? (totalTripSeconds / tripCount).toFixed(2) : "N/A";
//
// 		// Send response
// 		res.send({
// 			company: company,
// 			tripCount: tripCount,
// 			totalFare: totalFare.toFixed(2),
// 			totalTripSeconds: totalTripSeconds.toFixed(2),
// 			averageFare: avgFare,
// 			averageTripSeconds: avgTripSeconds,
// 			topPickupArea: topPickupArea,
// 			topDropoffArea: topDropoffArea
// 		});
// 	});
// });

// app.get('/company-stats', async function (req, res) {
// 	const company = req.query['company'];
// 	if (!company) {
// 		res.status(400).send("Company name is required");
// 		return;
// 	}
//
// 	console.log(company);
// 	// Query speed layer table
// 	hclient.table('shichen_taxi_trips_speed')
// 		.scan(
// 			{
// 				filter: {
// 					"type": "SingleColumnValueFilter",
// 					"family": Buffer.from('trip_info').toString('base64'),
// 					// "family": 'trip_info',
// 					"qualifier": Buffer.from('company').toString('base64'),
// 					// "qualifier": 'company',
// 					"op": "EQUAL",
// 					"comparator": {
// 						"type": "BinaryComparator",
// 						"value": Buffer.from(company).toString('base64')
// 					}
// 				}
// 			},
// 			function (err, rows) {
// 				if (err) {
// 					console.error("Error querying HBase:", err);
// 					return;
// 				}
// 				if (rows && rows.length > 0) {
// 					rows.forEach(row => {
// 						console.log(row);
// 					});
// 				} else {
// 					console.log("No rows found.");
// 				}
// 			}
// 		);
// });















/* Send simulated weather to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[4]});
var kafkaProducer = new Producer(kafkaClient);


app.post('/submit-taxi-data', (req, res) => {
    const taxiData = {
		tripId: req.body.tripId || "UNKNOWN",
		taxiId: req.body.taxiId || "UNKNOWN",
		tripStartTimestamp: req.body.tripStartTimestamp || new Date().toISOString(),
		tripEndTimestamp: req.body.tripEndTimestamp || new Date().toISOString(),
		tripSeconds: req.body.tripSeconds || 0,
		tripMiles: req.body.tripMiles || 0.0,
		pickupCensusTract: req.body.pickupCensusTract || "UNKNOWN",
		dropoffCensusTract: req.body.dropoffCensusTract || "UNKNOWN",
		pickupCommunityArea: req.body.pickupCommunityArea || 0,
		dropoffCommunityArea: req.body.dropoffCommunityArea || 0,
		fare: req.body.fare || 0.0,
		tips: req.body.tips || 0.0,
		tolls: req.body.tolls || 0.0,
		extras: req.body.extras || 0.0,
		tripTotal: req.body.tripTotal || 0.0,
		paymentType: req.body.paymentType || "CASH",
		company: req.body.company || "UNKNOWN",
		pickupCentroidLatitude: req.body.pickupCentroidLatitude || 0.0,
		pickupCentroidLongitude: req.body.pickupCentroidLongitude || 0.0,
		pickupCentroidLocation: req.body.pickupCentroidLocation || "UNKNOWN",
		dropoffCentroidLatitude: req.body.dropoffCentroidLatitude || 0.0,
		dropoffCentroidLongitude: req.body.dropoffCentroidLongitude || 0.0,
		dropoffCentroidLocation: req.body.dropoffCentroidLocation || "UNKNOWN"
	};

    kafkaProducer.send(
        [
            {
                topic: 'shichen-taxi-trip-events',
                messages: JSON.stringify(taxiData)
            }
        ],
        (err, data) => {
            if (err) {
                console.error('Kafka error:', err);
                res.status(500).send('Failed to send data to Kafka');
            } else {
                console.log('Kafka response:', data);
                res.status(200).send('Taxi data submitted successfully!');
            }
        }
    );
});

app.get('/weather.html',function (req, res) {
	var station_val = req.query['station'];
	var fog_val = (req.query['fog']) ? true : false;
	var rain_val = (req.query['rain']) ? true : false;
	var snow_val = (req.query['snow']) ? true : false;
	var hail_val = (req.query['hail']) ? true : false;
	var thunder_val = (req.query['thunder']) ? true : false;
	var tornado_val = (req.query['tornado']) ? true : false;
	var report = {
		station : station_val,
		clear : !fog_val && !rain_val && !snow_val && !hail_val && !thunder_val && !tornado_val,
		fog : fog_val,
		rain : rain_val,
		snow : snow_val,
		hail : hail_val,
		thunder : thunder_val,
		tornado : tornado_val
	};

	kafkaProducer.send([{ topic: 'weather-reports', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log(report);
			res.redirect('submit-weather.html');
		});
});

app.listen(port);
