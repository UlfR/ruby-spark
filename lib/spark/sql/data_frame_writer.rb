module Spark
  module SQL
    class DataFrameWriter

      attr_reader :sql_context, :jwriter

      def initialize(df)
        @sql_context = df.sql_context
        @jwriter = df.jdf.write
      end

      # Specifies the input data source format.
      # Parameter is name of the data source, e.g. 'json', 'parquet'.
      def format(source)
        jwriter.format(source)
        self
      end

      # Adds an input option for the underlying data source.
      def option(key, value)
        jwriter.option(key, value.to_s)
        self
      end

      # Adds input options for the underlying data source.
      def options(options)
        options.each do |key, value|
          jwriter.option(key, value.to_s)
        end
        self
      end

      # Loads data from a data source and returns it as a :class`DataFrame`.
      #
      # == Parameters:
      # path:: Optional string for file-system backed data sources.
      # format:: Optional string for format of the data source. Default to 'parquet'.
      # schema:: Optional {StructType} for the input schema.
      # options:: All other string options.
      #
      def save(path=nil, new_format=nil, new_options=nil)
        new_format && format(new_format)
        new_options && options(new_options)

        # TODO - jwrite returns nil, probably should catch exception and return true/false
        if path.nil?
          jwriter.save
        else
          jwriter.save(path)
        end
      end

      # Saves DataFrame as a JSON file (one object per line)
      #
      # == Parameters:
      # path:: string, path to the JSON dataset
      #
      # == Example:
      #   df = sql.writer.json('output.json')
      #
      def json(path)
        # ClassNotFoundException: Failed to load class for data source: json
        # df(jwriter.json(path))

        save(path, 'org.apache.spark.sql.execution.datasources.json')
      end

      def parquet(path)
        # ClassNotFoundException: Failed to load class for data source: parquet
        # df(jwriter.parquet(path))

        save(path, 'org.apache.spark.sql.execution.datasources.parquet')
      end

      def orc(path)
        # ClassNotFoundException: Failed to load class for data source: json
        # df(jwriter.json(path))

        save(path, 'org.apache.spark.sql.execution.datasources.orc')
      end

      def insert_into table_name
        jwriter.insertInto(table_name)
      end

      def save_as_table name
        jwriter.saveAsTable(name)
      end

      def partition_by columns
        jwriter.partitionBy(columns)
      end

      def mode name
        jwriter.mode(name)
        self
      end

    end
  end
end
