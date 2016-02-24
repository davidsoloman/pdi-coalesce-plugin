# Coalesce Step - PDI Plugin

This step allows you to select the first non null value from a set of given fields <br>
Click [here](http://www.w3resource.com/mysql/comparision-functions-and-operators/coalesce-function.php) for an example of MySql Coalesce function

## Help

Help for the step and its usage can be found in the [Help documentation](help.md).


## Development

### Build
To build (requires Apache Maven 3 or later):

```shell
mvn package
```

### Install

1. At the project level location run:

    ```
    mvn install -Dpdi.home=/path/to/data-integration
    ```
2. Restart spoon for the changes to take effect

## Authors:
- [Emil Anca](https://github.com/emilanca) - emil (dot) anca (at) ymail (dot) com
