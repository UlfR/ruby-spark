module Spark
  module SQL
    class Context

      attr_reader :spark_context, :jsql_context

      def initialize(spark_context)
        @spark_context = spark_context
        @jsql_context = JSQLContext.new(spark_context.sc)
      end

      def read
        DataFrameReader.new(self)
      end

      def sql query
        DataFrame.new(jsql_context.sql(query).toDF, self)
      end

    end
  end
end
