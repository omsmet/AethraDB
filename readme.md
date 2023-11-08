# AethraDB: An In-Process OLAP Query Processing Engine
This repository contains the source-code for the AethraDB in-process OLAP query processing.
This engine was created in the process of my MSc graduation project, which mainly focusses on assessing how code generation techniques can be applied to optimise in-process OLAP query execution.
The engine supports execution of SQL queries in both the data-centric query compilation paradigm, as well as the vectorised execution style.
For more information on the exact capabilities of the engine, as well as research outcomes, I refer to my MSc thesis report.

It is important to remark that this repository does not exist in a vacuum and that several artifacts exist in separate repositories:
- This repository contains the source code for the AethraDB code generation engine, as well as the AethraDB execution system.
- The Apache Calcite based AethraDB query planning library can be found in the [AethraDB-Planner-Lib](https://github.com/omsmet/AethraDB-Planner-Lib) repository.
- To make the planner library compatible with GraalVM Native Image techology, some small adaptations have been made to Calcite, and this adapted library can be found in the [AethraDB-Calcite](https://github.com/omsmet/AethraDB-Calcite) repository.
- Finally, all artifacts concerning benchmarking, which include data generators, benchmarking scripts, benchmarking results and benchmarking data on competitive engines can be found in the [AethraDB-Support](https://github.com/omsmet/AethraDB-Support) repository.