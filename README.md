# NetCoreSqlPerf

## Repro Steps
1. Clone repo
2. Create appsettings.json with connection string

    ```json
    {
      "ConnectionStrings": {
        "DefaultConnection": "Server=foo;Database=bar;User Id=baz;Password=qux"
      }
    }
    ```

3. Edit project.json to select the desired version of EF
4. `dotnet run -c release -- [SqlDataReader|EF|EFSingleTable]`
 1. SQL tables will be created and populated when app is first run
