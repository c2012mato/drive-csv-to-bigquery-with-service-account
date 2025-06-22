# drive-csv-to-bigquery-with-service-account
This project collects and combines all csvs in target folder and load to bigquery as table

# Flow Diagram

```mermaid

graph TD
    style StartNode fill:#3498db,stroke:#333,stroke-width:2px;
    style EndNode fill:#3498db,stroke:#333,stroke-width:2px;
    style ConfigNode fill:#95a5a6,stroke:#333,stroke-width:2px;
    style BQNode fill:#e74c3c,stroke:#333,stroke-width:2px;
    style FetchNode fill:#f39c12,stroke:#333,stroke-width:2px;
    style LoopNode fill:#95a5a6,stroke:#333,stroke-width:2px;
    style DataNode fill:#95a5a6,stroke:#333,stroke-width:2px;
    style PipelineNode fill:#95a5a6,stroke:#333,stroke-width:2px;
    style ErrorNode fill:#e74c3c,stroke:#333,stroke-width:2px;

    A[Start]:::StartNode --> B[Connect to Google Drive]:::ConfigNode
    B --> C[Load Config]:::ConfigNode
    C --> D[Instantiate Pipeline]:::PipelineNode
    D --> E[Move Excel/CSV Files<br>to Year/Month Folders]:::DataNode
    E --> F[Find All Files in Folder]:::DataNode
    F --> G{Loop: Each File}:::LoopNode
    G --> H[Download File as DataFrame]:::FetchNode
    H --> I[Add Filename and Upload Date]:::DataNode
    I --> J[Parse Date Columns]:::DataNode
    J --> K[Determine Target Table<br>by Filename Prefix]:::ConfigNode
    K --> L[Retrieve Last Call Timestamp<br>from BigQuery]:::BQNode
    L --> M[Filter Rows After Last Timestamp]:::DataNode
    M --> N[Upload DataFrame to BigQuery]:::BQNode
    N --> O[Next File?]:::LoopNode
    O -- Yes --> G
    O -- No --> P[Pipeline Complete]:::EndNode
    P --> Q[End]:::EndNode
    G -- Error --> X[Log/Handle Error]:::ErrorNode
    X --> O

    %% Styling for main nodes (works in Mermaid Live)
    classDef StartNode fill:#3498db,stroke:#333,stroke-width:2px;
    classDef EndNode fill:#3498db,stroke:#333,stroke-width:2px;
    classDef ConfigNode fill:#95a5a6,stroke:#333,stroke-width:2px;
    classDef BQNode fill:#e74c3c,stroke:#333,stroke-width:2px;
    classDef FetchNode fill:#f39c12,stroke:#333,stroke-width:2px;
    classDef DataNode fill:#95a5a6,stroke:#333,stroke-width:2px;
    classDef PipelineNode fill:#95a5a6,stroke:#333,stroke-width:2px;
    classDef LoopNode fill:#95a5a6,stroke:#333,stroke-width:2px;
    classDef ErrorNode fill:#e74c3c,stroke:#333,stroke-width:2px;
