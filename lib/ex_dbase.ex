defmodule ExDbase do
  @moduledoc """
  Elixir library to parse Dbase III (.dbf) files
  """

  alias Decimal, as: D

  @decimal_zero D.new("0.00")

  @doc """
  Takes .dbf file and return a list of records.

  Each record will be map in the folowing format "%{column_name => data}".

  ## Option
  * `:columns` - List of field names to be extracted from each record, which defaults to "[]" i.e. all.
  * `:map_fn` - Anonymous map fn to transform each record, which defaults to "fn x -> x end".
  """
  def parse(dbf_file, columns \\ [], map_fn \\ fn x -> x end) do
    dbf_file
    |> File.read!()
    |> parse_binary(columns, map_fn)
  end

  @doc """
  Takes dbf binary content and return a list of records.

  Each record will be map in the folowing format "%{column_name => data}".

  ## Option
  * `:columns` - List of field names to be extracted from each record, which defaults to "[]" i.e. all.
  * `:map_fn` - Anonymous map fn to transform each record, which defaults to "fn x -> x end".
  """
  def parse_binary(data, columns \\ [], map_fn \\ fn x -> x end) do
    <<
      _version,
      _last_updated::3-bytes,
      rec_count::32-unsigned-little-integer,
      _header_size::16-unsigned-little-integer,
      a_rec_size::16-unsigned-little-integer,
      _reserved::2-bytes,
      _incomplete_tx_flag,
      _encryption_flag,
      _multi_user_reserved::12-bytes,
      _mdx_flag,
      _lang_driver_id,
      _reserverd::2-bytes,
      rest::binary
    >> = data

    {fields, _field_count, rest} = parse_fields(rest)

    if columns === [] do
      parse_records(rest, fields, [], map_fn, a_rec_size - 1, rec_count, [])
    else
      parse_records(rest, fields, MapSet.new(columns), map_fn, a_rec_size - 1, rec_count, [])
    end
  end

  defp parse_records(_bin, _fields, _columns, _map_fn, _a_rec_size, 0, records) do
    :lists.reverse(records)
  end

  # Maybe check if size of remaining bin > a_rec_size else stop processing since record is incomplete
  defp parse_records(bin, fields, columns, map_fn, a_rec_size, rec_count, records) do
    <<deleted::1-bytes, rec::binary-size(a_rec_size), rest::binary>> = bin

    if deleted === " " do
      case rec |> parse_record_by_fields(fields, columns, %{}) |> map_fn.() do
        nil ->
          parse_records(rest, fields, columns, map_fn, a_rec_size, rec_count - 1, records)

        parsed_record ->
          parse_records(rest, fields, columns, map_fn, a_rec_size, rec_count - 1, [
            parsed_record | records
          ])
      end
    else
      parse_records(rest, fields, columns, map_fn, a_rec_size, rec_count - 1, records)
    end
  end

  defp parse_record_by_fields(_, [], _, acc), do: acc

  defp parse_record_by_fields(rec, fields, [], acc) do
    [field | f] = fields
    len = field.length
    <<data::binary-size(len), rest::binary>> = rec
    data = data |> String.trim() |> parse_data(field.type, field.decimal_count)
    parse_record_by_fields(rest, f, [], Map.put(acc, field.name, data))
  end

  defp parse_record_by_fields(rec, fields, columns, acc) do
    [field | f] = fields
    len = field.length
    <<data::binary-size(len), rest::binary>> = rec

    if MapSet.member?(columns, field.name) do
      data = data |> String.trim() |> parse_data(field.type, field.decimal_count)
      parse_record_by_fields(rest, f, columns, Map.put(acc, field.name, data))
    else
      parse_record_by_fields(rest, f, columns, acc)
    end
  end

  # Parse data based on data_type i.e. Character, Integer, Numeric etc
  defp parse_data(data, "C", _decimal_count), do: data
  defp parse_data(data, "D", _decimal_count), do: data
  defp parse_data(_data, "M", _decimal_count), do: nil

  defp parse_data("", "N", 0), do: 0

  defp parse_data(data, "N", 0) do
    String.to_integer(data)
  catch
    _ -> 0
  end

  defp parse_data("", "N", _), do: @decimal_zero

  defp parse_data(data, "N", _) do
    D.new(data)
  catch
    _ -> @decimal_zero
  end

  # There will always be 1 field/column for a table
  defp parse_fields(<<field_header::32-bytes, rest::binary>>), do: parse_fields([parse_field(field_header)], 1, rest)

  defp parse_fields(fields, count, bin) do
    <<stop::1-bytes, rest::binary>> = bin

    if stop === <<13>> do
      <<zero::1-bytes, records::binary>> = rest

      if zero === <<0>> do
        {:lists.reverse(fields), count, records}
      else
        {:lists.reverse(fields), count, rest}
      end
    else
      <<field_header::32-bytes, rest::binary>> = bin
      field = parse_field(field_header)
      parse_fields([field | fields], count + 1, rest)
    end
  end

  defp parse_field(
         <<name::11-bytes, type, _bytes_to_records::4-bytes, length::8-unsigned-little-integer,
           decimal_count::8-unsigned-little-integer, _::14-bytes>>
       ),
       do: %{
         name: parse_field_name(name),
         type: :binary.list_to_bin([type]),
         length: length,
         decimal_count: decimal_count
       }

  # Trim off null / zero bytes padding if any
  defp parse_field_name(bin), do: hd(:binary.split(bin, <<0>>))

  @doc """
  Takes a .dbf file and return a map of fields / columns types
  """
  def field_info(dbf_file) do
    header = header_info(dbf_file)
    header_size = Keyword.get(header, :header_size)

    {:ok, dbf} = :file.open(dbf_file, [:raw, :binary])
    # header_size - 31 is to account for the empty or stop byte <<13>> | 0x0D
    {:ok, fields} = :file.pread(dbf, 32, header_size - 31)
    :file.close(dbf_file)
    {fields, _field_count, _empty_byte} = parse_fields(fields)
    fields
  end

  @doc """
  Takes a .dbf file and return a keyword list of the header data.
  """
  def header_info(dbf_file) do
    {:ok, dbf} = :file.open(dbf_file, [:raw, :binary])
    {:ok, header} = :file.read(dbf, 32)
    :file.close(dbf_file)

    <<
      # version flag
      version::1-bytes,
      # date of last update in YYMMDD format
      last_updated::3-bytes,
      # number of records in the table
      rec_count::32-unsigned-little-integer,
      # number of bytes in the header
      header_size::16-unsigned-little-integer,
      # number of bytes in the record
      a_rec_size::16-unsigned-little-integer,
      # reserved filled with zeros
      _reserved::2-bytes,
      # flag indicating incomplete dBASE IV transaction
      incomplete_tx_flag::1-bytes,
      # dBASE IV encryption flag
      encryption_flag::1-bytes,
      # reserved for multi-user processing
      multi_user_reserved::12-bytes,
      # mdx flag; 0x01 if .mdx file exists else 0x00
      mdx_flag::1-bytes,
      # language driver ID
      lang_driver_id::1-bytes,
      # reversed filled with zeros
      _reversed::2-bytes
    >> = header

    [
      version: version(version),
      last_updated: last_updated,
      rec_count: rec_count,
      header_size: header_size,
      a_rec_size: a_rec_size,
      incomplete_tx_flag: incomplete_tx_flag,
      encryption_flag: encryption_flag,
      multi_user_reserved: multi_user_reserved,
      mdx_flag: mdx_flag,
      lang_driver_id: lang_driver_id
    ]
  end

  defp version(bin) do
    [v] = :binary.bin_to_list(bin)

    case v do
      0x02 -> "FoxBase 1.0"
      0x03 -> "FoxBase 2.x / dBASE III"
      0x83 -> "FoxBase 2.x / dBASE III with memo file"
      0x30 -> "Visual FoxPro"
      0x31 -> "Visual FoxPro with auto increment"
      0x32 -> "Visual FoxPro with varchar/varbinary"
      0x43 -> "dBASE IV SQL Table, no memo file"
      0x63 -> "dBASE IV SQL System, no memo file"
      0x8B -> "dBASE IV with memo file"
      0xCB -> "dBASE IV SQL Table with memo file"
      0xFB -> "FoxPro 2"
      0xF5 -> "FoxPro 2 with memo file"
    end
  end
end
