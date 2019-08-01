// Imports
require("dotenv").config();
const axios = require("axios");
const mongoClient = require("mongodb").MongoClient;
// Required for axios x-www-form-urlencoded headers
const querystring = require("querystring");
// Nevermind, we're using request now
const request = require("request");

const apiUrl = `${process.env.ARM_ENDPOINT}subscriptions/${
  process.env.SUBSCRIPTION_ID
}/resourceGroups/${
  process.env.RESOURCE_GROUP
}/providers/Microsoft.Media/mediaServices/${process.env.ACCOUNT_NAME}`;

const assetUrl = `${apiUrl}/assets`;
const streamingUrl = `${apiUrl}/streamingLocators`;
const basePath = "//postsms-usw22.streaming.media.azure.net";

this.access_token;

function main() {
  this.streamingLinks = [];
  this.failPasses = 1;
  // Get all azure assets
  //// Get token
  console.log("Attempting to obtain access token from Azure...");
  getAzureAccessToken(() => {
    //// On token obtained
    getStreamingLinks(links => {
      // On Links obtained
      postNewVideosToMongo(links);
    });
  });
  let azureAssets = {};

  // Get all VIDEO_DB assets
  // Find which assets do not have an associated document in Mongo
  // Create Mongo document for each asset not in mongo
  // Insert documents into mongo
  // Test
}

function getAzureAccessToken(onComplete) {
  axios
    .post(
      `${process.env.ENDPOINT}/${process.env.TENANT_ID}/oauth2/token`,
      querystring.stringify({
        grant_type: "client_credentials",
        client_id: process.env.CLIENT_ID,
        client_secret: process.env.SECRET,
        resource: "https://management.core.windows.net/"
      })
    )
    .then(function(res) {
      if (!res.data.access_token) {
        console.log("Access token wasn't present on azure response!");
        console.log("Exiting");
      } else {
        console.log("Obtained access token from Azure");
        this.access_token = res.data.access_token;
        onComplete();
      }
    })
    .catch(function(error) {
      console.log("Error", error);
    });
}

function getStreamingLinks(onComplete) {
  console.log("Getting assets from Azure");

  let options = {
    method: "GET",
    url: assetUrl,
    qs: { "api-version": "2018-07-01" },
    headers: {
      Authorization: "Bearer " + this.access_token,
      "Content-Type": "application/json",
      Accept: "application/json"
    }
  };
  request(options, function(error, response, body) {
    if (error) throw new Error(error);
    console.log("Obtained Azure assets!");
    console.log("Converting to links object...");
    // Convert to links
    convertToLinksObject(JSON.parse(response.body).value, links => {
      onComplete(links);
    });
  });
}

function convertToLinksObject(assets, onComplete) {
  console.log(`\nPass number: ${this.failPasses}`);
  //   For each asset, get the steamingLocator
  let requestCount = assets.length;
  let failedRequests = [];
  assets.forEach(asset => {
    let assetOptions = {
      method: "POST",
      url: `${assetUrl}/${asset.properties.assetId}/listStreamingLocators`,
      qs: { "api-version": "2018-07-01" },
      headers: {
        "cache-control": "no-cache",
        Authorization: "Bearer " + this.access_token,
        "Content-Type": "application/json",
        Accept: "application/json"
      }
    };

    let streamingOptions = function(streamingLocator) {
      return {
        method: "POST",
        url: `${streamingUrl}/${streamingLocator}/listPaths`,
        qs: { "api-version": "2018-07-01" },
        headers: {
          "cache-control": "no-cache",
          Authorization: "Bearer " + this.access_token,
          "Content-Type": "application/json",
          Accept: "application/json"
        }
      };
    };

    request(assetOptions, (error, resp, body) => {
      if (resp && resp.body) {
        let res = JSON.parse(resp.body);
        if (!res.error) {
          if (res.streamingLocators.length > 0) {
            // Get the steamingPaths
            request(
              streamingOptions(res.streamingLocators[0].streamingLocatorId),
              (error, response, body) => {
                if (response && response.body) {
                  let res = JSON.parse(response.body);
                  if (!res.error) {
                    this.streamingLinks.push({
                      paths: res.streamingPaths,
                      downloadPaths: res.downloadPaths,
                      assetID: asset.properties.assetId,
                      description: asset.properties.description
                    });
                    if (--requestCount === 0) {
                      if (failedRequests.length > 0) {
                        console.log(
                          `\n${failedRequests.length} failed requests...\n`
                        );
                        this.failPasses++;
                        convertToLinksObject(failedRequests, onComplete);
                      } else {
                        onComplete(this.streamingLinks);
                      }
                    } else {
                      process.stdout.write("\r\x1b[K");
                      process.stdout.write(`${requestCount} assets left`);
                    }
                  } else {
                    failedRequests.push(asset);
                    if (--requestCount === 0) {
                      if (failedRequests.length > 0) {
                        console.log(
                          `\n${failedRequests.length} failed requests...\n`
                        );
                        this.failPasses++;
                        convertToLinksObject(failedRequests, onComplete);
                      } else {
                        onComplete(this.streamingLinks);
                      }
                    } else {
                      process.stdout.write("\r\x1b[K");
                      process.stdout.write(`${requestCount} assets left`);
                    }
                  }
                } else {
                  if (--requestCount === 0) {
                    if (failedRequests.length > 0) {
                      console.log(
                        `\n${failedRequests.length} failed requests...\n`
                      );
                      this.failPasses++;
                      convertToLinksObject(failedRequests, onComplete);
                    } else {
                      onComplete(this.streamingLinks);
                    }
                  } else {
                    process.stdout.write("\r\x1b[K");
                    process.stdout.write(`${requestCount} assets left`);
                  }
                }
              }
            );
          } else {
            if (res.streamingLocators.length > 1) {
              console.log(
                `Found locator with ${res.streamingLocators.length} locators`
              );
            }
            if (--requestCount === 0) {
              if (failedRequests.length > 0) {
                console.log(`\n${failedRequests.length} failed requests...\n`);
                this.failPasses++;
                convertToLinksObject(failedRequests, onComplete);
              } else {
                onComplete(this.streamingLinks);
              }
            } else {
              process.stdout.write("\r\x1b[K");
              process.stdout.write(`${requestCount} assets left`);
            }
          }
        } else {
          failedRequests.push(asset);
          if (--requestCount === 0) {
            if (failedRequests.length > 0) {
              console.log(`\n${failedRequests.length} failed requests...\n`);
              this.failPasses++;
              convertToLinksObject(failedRequests, onComplete);
            } else {
              onComplete(this.streamingLinks);
            }
          } else {
            process.stdout.write("\r\x1b[K");
            process.stdout.write(`${requestCount} assets left`);
          }
        }
      } else {
        if (--requestCount === 0) {
          if (failedRequests.length > 0) {
            console.log(`\n${failedRequests.length} failed requests...\n`);
            this.failPasses++;
            convertToLinksObject(failedRequests, onComplete);
          } else {
            onComplete(this.streamingLinks);
          }
        } else {
          process.stdout.write("\r\x1b[K");
          process.stdout.write(`${requestCount} assets left`);
        }
      }
    });
  });
}

function postNewVideosToMongo(links) {
  console.log(`\nFound ${links.length} assets with valid streaming links`);
  getAllMongoDocuments(function(results) {
    let newVideos = getNewVideos(links, results);
    let newMongoDocuments = makeNewMongoDocuments(newVideos);
    console.log(
      `${newMongoDocuments.length} videos queued to insert into the database`
    );
    addNewDocsToMongo(newMongoDocuments);
  });
}

function getAllMongoDocuments(onComplete) {
  mongoClient.connect(
    process.env.VIDEO_DB_PRIMARY_CONNECTION_STRING,
    function(err, db) {
      if (err) console.log(err);
      var dbo = db.db(process.env.VIDEO_DB);
      dbo
        .collection(process.env.VIDEO_DB_COLLECTION)
        .find({})
        .toArray(function(err, result) {
          if (err) console.log(err);
          onComplete(result);
          db.close();
        });
    }
  );
}

function getNewVideos(links, mongoDocs) {
  console.log("Checking for new assetIDs");
  let newVideos = [];
  let mongoArray = mongoDocs;
  links.forEach(function(item) {
    // Search until we find the document
    let mongoDocs = mongoArray;
    if (mongoDocs.length !== 0) {
      mongoDocs.some(function(doc, index) {
        if (doc.assetID && doc.assetID === item.assetID) {
          mongoArray.splice(index, 1);
        } else {
          newVideos.push(item);
          return true;
        }
      });
    } else {
      newVideos.push(item);
    }
  });
  console.log(`Found ${newVideos.length} assets that are not in the database`);
  return newVideos;
}

function makeNewMongoDocuments(newVideos) {
  let preferredNames = {
    Hls: "hls",
    Dash: "dash"
  };
  let ccCount = 1;
  let dedCount = 1;
  let newVideoDocuments = [];
  newVideos.forEach(function(video) {
    // Make document for each video
    // Check if closed captioning asset
    if (
      !video.downloadPaths.some(function(path) {
        return path.indexOf(".vtt") === path.length - 4 && ccCount++;
      }) &&
      video.paths[0].paths.length > 0 // because sometimes the assets claim they have streaming links WHEN THEY DONT
    ) {
      // Make mp4 array
      let mp4Paths = [];
      video.downloadPaths.forEach(function(videoLink) {
        mp4Paths.push(`${basePath}${videoLink}`);
      });
      // Make links object
      let links = {};
      video.paths.forEach(function(pathObj) {
        if (preferredNames[pathObj.streamingProtocol]) {
          links[preferredNames[pathObj.streamingProtocol]] = [];
          pathObj.paths.forEach(function(path) {
            links[preferredNames[pathObj.streamingProtocol]].push(
              `${basePath}${path}`
            );
          });
        }
      });
      // Append mp4 array to links obj
      Object.assign(links, { mp4: mp4Paths });
      let doc = {
        title: video.description,
        description: video.description,
        assetID: video.assetID,
        links: links,
        status: "unreleased"
      };

      newVideoDocuments.push(doc);
    } else {
      dedCount++;
    }
  });
  console.log(`Found ${--ccCount} closed caption assets`);
  console.log(
    `Found ${--dedCount} assets that don't actually have valid streaming paths`
  );
  return newVideoDocuments;
}

function addNewDocsToMongo(newDocs) {
  mongoClient.connect(
    process.env.VIDEO_DB_PRIMARY_CONNECTION_STRING,
    function(err, db) {
      if (err) console.log(err);
      var dbo = db.db(process.env.VIDEO_DB);
      dbo
        .collection(process.env.VIDEO_DB_COLLECTION)
        .insertMany(newDocs, function(err, result) {
          if (err) console.log(err);
          console.log(`Success! ${result.insertedCount} documents inserted`);
          db.close();
        });
    }
  );
}

// mongoClient.connect(
//   process.env.VIDEO_DB_PRIMARY_CONNECTION_STRING,
//   function(err, db) {
//     if (err) throw err;
//     var dbo = db.db("VIDEO_DB");
//     dbo.createCollection("videos", function(err, res) {
//       if (err) throw err;
//       console.log("Collection created!");
//       db.close();
//     });
//   }
// );

main();
