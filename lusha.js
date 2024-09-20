const fs = require("node:fs");
const csvtojsonV2 = require("csvtojson");
const _ = require("lodash");
const { google } = require("googleapis");
const { GoogleAuth } = require("google-auth-library");
const company = []; // Track industries
const path = "C:/Users/firzan/Downloads/lusha.csv";
const snowFlakeSpreadsheetID = "1cnAdyVzVVQvJyRqmJe2ea8zHf5xZ0DUFGULTWPADGrE";
const snowFlakeSpreadsheetRange = "sheet1";
const finalSpreadsheetID = "1ZAo3sn37Y_kZ64M9mrYgHT-8u41Tlf5k5o2Vuy2cwj8";
const serviceAccountKey = "C:/Users/firzan/Downloads/lusha-sa.json";
const googleSheetScopes = "https://www.googleapis.com/auth/spreadsheets";

// Initialize Google Sheet service
const googleSheetService = async () => {
  const auth = new GoogleAuth({
    keyFile: serviceAccountKey,
    scopes: googleSheetScopes,
  });

  const service = google.sheets({
    version: "v4",
    auth: await auth.getClient(),
  });

  return service.spreadsheets.values;
};

const processLushaData = async (lushaData) => {
  const processedLushaData = lushaData.map((d) => {
    const companyName = d["Company name"];
    const companyIndustry = d["Company main industry"];

    return {
      "": "",
      "Company name": companyName, // Only show for first entry
      "Company main industry": companyIndustry, // Only show for first industry entry
      "Full name": `${d["First name"]} ${d["Last name"]}`,
      "Job title": d["Job title"],
      "Phone 1": d["Phone 1"] ? d["Phone 1"].replace(/\s+/g, "") : "", // Remove spaces
      "Work Email": d["Work Email"],
      "Linkedin URL": d["Linkedin URL"],
      "Phone 2": d["Phone 2"] ? d["Phone 2"].replace(/\s+/g, "") : "", // Remove spaces (optional)
      Seniority: d["Seniority"], // optional
      "Company Linkedin URL": d["Company Linkedin URL"], // optional
      Country: d["Country"], // optional
      "Company country": d["Company country"], // optional
    };
  });

  const finalLushaData = processedLushaData.map((d) => Object.values(d));
  return finalLushaData;
};

// Read and filter lusha data
const readLushaData = async () => {
  const lushaData = await csvtojsonV2().fromFile(path);

  const finalLushaData = await processLushaData(lushaData);

  return finalLushaData;
};

// Read snowflake data
const readSnowFlakeData = async () => {
  const googleSheet = await googleSheetService();
  const result = await googleSheet.get({
    spreadsheetId: snowFlakeSpreadsheetID,
    range: snowFlakeSpreadsheetRange,
  });

  return result.data.values;
};

// Merge Lusha into Snowflake
const mergeLushaIntoSnowflake = async (lushaData, snowflakeData) => {
  try {
    let inCharge = "";
    let com = "";
    let field = "";

    // Step 2: mapping to follow Lusha structure
    snowflakeData.forEach((sl, index) => {
      sl[13] = sl[9]; // Status change to column 14
      sl[12] = ""; // Add column 12 ()
      sl[11] = ""; // Add column 10 ()
      sl[10] = ""; // Add column 11 ()
      sl[9] = ""; // Add column 10 ()
      sl[8] = ""; // Add column 9 ()
      sl[7] = ""; // Add column 8 ()

      if (!!sl[0] && sl[0] != "") {
        inCharge = sl[0];
      }

      if (!!sl[2] && sl[2] != "") {
        field = sl[2];
      }

      if (!company.includes(sl[1]) && !!sl[1] && sl[1] != "") {
        com = sl[1];
        company.push({ name: sl[1], inCharge, data: [sl] });
      }

      if ("" === sl[1]) company[company.length - 1].data.push(sl);

      // Denormalize, all data now have incharge, companyName & industry
      sl[2] = field;
      sl[1] = com;
      sl[0] = inCharge;
    });

    snowflakeData.forEach((s, index) => {
      lushaData.forEach((v) => {
        if (s[1].toLowerCase() === v[1].toLowerCase()) {
          v[0] = s[0];
          v[2] = s[2];
        }

        // Match company names, case-insensitive && unchecked lusha data will pass through
        if (s[1].toLowerCase() === v[1].toLowerCase() && !v.checked) {
          // Find the last occurrence of this company in snowflakeList
          let lastIndex = index;
          for (let i = index + 1; i < snowflakeData.length; i++) {
            if (snowflakeData[i][1].toLowerCase() === s[1].toLowerCase()) {
              lastIndex = i;
            } else {
              break; // Stop when the company no longer matches
            }
          }

          // copy lusha value to avoid checked attribute
          const a = v;

          // Merged match company data list
          snowflakeData.splice(lastIndex + 1, 0, a); // Insert the `v` after lastIndex

          // Checked is true for lusha data that already merged into snowflake
          v.checked = true;
        }
      });
    });

    return snowflakeData;
  } catch (err) {
    console.error("Error appending data:", err);
    throw err;
  }
};

const appendData = async (updatedData) => {
  try {
    const googleSheet = await googleSheetService();

    await googleSheet.append({
      spreadsheetId: finalSpreadsheetID,
      range: "A1",
      valueInputOption: "USER_ENTERED",
      resource: { values: updatedData },
    });
  } catch (err) {
    console.error("Error appending data:", err);
    throw err;
  }
};

const emptyDuplicatedCell = async (snowflakeListData) => {
  const seen = new Set(); // To keep track of unique entries
  const foundNames = new Set();
  const foundIndustry = new Set();
  const foundCompany = new Set();

  snowflakeListData.forEach((row, index) => {
    // Trim and join first 3 fields (name, company, industry) to create a unique key
    const key = row
      .slice(0, 3)
      .map((field) => field.trim())
      .join("|")
      .toLowerCase();

    if (index !== 0 && seen.has(key)) {
      // Replace first 3 fields with empty strings if the key has been seen
      row[0] = "";
      row[1] = "";
      row[2] = "";
    } else {
      seen.add(key); // Add to the set if it's the first occurrence
    }

    if (!foundNames.has(row[0].toLowerCase())) {
      foundNames.add(row[0].toLowerCase()); // Add the name to the set
    } else {
      // Empty the first three fields for subsequent occurrences
      row[0] = ""; // Empty the first three fields
    }

    if (!foundCompany.has(row[1].toLowerCase())) {
      foundCompany.add(row[1].toLowerCase()); // Add the name to the set
    } else {
      // Empty the first three fields for subsequent occurrences
      row[1] = ""; // Empty the first three fields
    }

    if (!foundIndustry.has(row[2].toLowerCase())) {
      foundIndustry.add(row[2].toLowerCase()); // Add the name to the set
    } else {
      // Empty the first three fields for subsequent occurrences
      row[2] = ""; // Empty the first three fields
    }
  });

  return snowflakeListData;
};

const mappingProcess = async () => {
  // Step 1: read from lusha
  const lushaData = await readLushaData();

  // Step 2: Read data from snowflake csv
  const snowflakeData = await readSnowFlakeData();

  // Step 3: merge the lusha into snowflake existing data, snowflake will go through denormalize process
  const mergedData = await mergeLushaIntoSnowflake(lushaData, snowflakeData);

  // Step 4: Due to denormalization, we need to clean up the duplicate cell into empty string
  const cleanedData = await emptyDuplicatedCell(mergedData);

  // Step 5: append the data
  appendData(cleanedData);
};

// Run it!
mappingProcess();
