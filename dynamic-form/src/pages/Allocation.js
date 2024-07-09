import React, { useEffect, useState } from "react";
import "handsontable/dist/handsontable.full.min.css";
// import allocationJson from "data/allocation.json";
import allocationJson from "../data/allocation.json";
import Handsontable from "handsontable/base";
// import { registerAllModules } from "handsontable/registry";
import { HotTable } from "@handsontable/react";
import { extractData } from "../functions/app";

// registerAllModules();

const Allocation = () => {
  //   const tableData = extractData(allocationJson);
  const [tableData, setCsvData] = useState("");

  useEffect(() => {
    // fetch("/allocation.json")
    //   //   .then((response) => response.json())
    //   .then((json) => {
    const resultArray = extractData(allocationJson);
    const resultCSV = resultArray.join(",");
    setCsvData(resultCSV);
    //   })
    //   .catch((error) => console.error("Error fetching the JSON file:", error));

    console.log("tableData    ", tableData);
  }, [tableData]); // Empty dependency array means this effect runs once when the component mounts

  return (
    <div>
      <HotTable
        // data={[
        //   ["", "Tesla", "Volvo", "Toyota", "Ford"],
        //   ["2019", 10, 11, 12, 13],
        //   ["2020", 20, 11, 14, 13],
        //   ["2021", 30, 15, 12, 13],
        // ]}
        data={tableData}
        rowHeaders={true}
        colHeaders={true}
        height="auto"
        autoWrapRow={true}
        autoWrapCol={true}
        licenseKey="non-commercial-and-evaluation" // for non-commercial use only
      />
    </div>
  );
};

export default Allocation;
