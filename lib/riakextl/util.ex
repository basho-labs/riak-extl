defmodule RiakExtl.Util do
  def to_str(item, opts \\ []) do
	  opts = struct(Inspect.Opts, opts)
	  Inspect.Algebra.format(Inspect.Algebra.to_doc(item, opts), 500)
	end
end
