module Spark
  module Mllib
    class Matrix < BaseMatrix

      def self.dense(*args)
        DenseMatrix.new(*args)
      end

      def self.sparse(*args)
        SparseMatrix.new(*args)
      end

      def self.to_matrix(data)
        if data.is_a?(SparseMatrix) || data.is_a?(DenseMatrix)
          data
        elsif data.is_a?(Array)
          DenseMatrix.new(data)
        end
      end

    end
  end
end

##
# DenseMatrix
#
#   DenseMatrix.new(2, 3, [[1,2,3], [4,5,6]).values
#   # => [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]
#
module Spark
  module Mllib
    class DenseMatrix < Matrix

      def initialize(rows, cols, values)
        super('dense', rows, cols, values.to_a)
      end

    end
  end
end

##
# SparseMatrix
#
# == Arguments:
# rows::
#   Number of rows.
#
# cols::
#   Number of columns.
#
# col_pointers::
#   The index corresponding to the start of a new column.
#
# row_indices::
#   The row index of the entry. They must be in strictly
#   increasing order for each column.
#
# values::
#   Nonzero matrix entries in column major.
#
# == Examples:
#
#   SparseMatrix.new(3, 3, [0, 2, 3, 6], [0, 2, 1, 0, 1, 2], [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).values
#
#   # => [
#   #      [1.0, 0.0, 4.0],
#   #      [0.0, 3.0, 5.0],
#   #      [2.0, 0.0, 6.0]
#   #    ]
#
module Spark
  module Mllib
    class SparseMatrix < Matrix

      attr_reader :col_pointers, :row_indices

      def initialize(rows, cols, col_pointers, row_indices, values)
        super('sparse', rows, cols)

        @col_pointers = col_pointers
        @row_indices = row_indices
        @_values = values

        j = 0
        while j < cols
          idx = col_pointers[j]
          idx_end = col_pointers[j+1]
          while idx < idx_end
            self[row_indices[idx], j] = values[idx]
            idx += 1
          end
          j += 1
        end
      end

    end
  end
end