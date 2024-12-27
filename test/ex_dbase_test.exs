defmodule ExDbaseTest do
  use ExUnit.Case

  @sample_dbf "test/fixtures/sample.dbf"

  describe "parse/3" do
    test "should parse and read dbf file" do
      assert [
               %{
                 "ASD" => nil,
                 "CITY" => "Kapaa Kauai",
                 "COUNTRY" => "U.s.a.",
                 "CUST_NO" => 1221,
                 "FRST_CNTCT" => "19900403",
                 "NAME" => "Kauai Dive Shoppe",
                 "PHONE" => "808-555-0269",
                 "STATE_PROV" => "Hi",
                 "STREET" => "4-976 Sugarloaf Hwy",
                 "ZIP_PST_CD" => "94766"
               },
               %{
                 "ASD" => nil,
                 "CITY" => "Freeport",
                 "COUNTRY" => "Bahamas",
                 "CUST_NO" => 1231,
                 "FRST_CNTCT" => "19810228",
                 "NAME" => "Unisco",
                 "PHONE" => "809-555-3915",
                 "STATE_PROV" => "",
                 "STREET" => "Po Box Z-547",
                 "ZIP_PST_CD" => ""
               },
               %{
                 "ASD" => nil,
                 "CITY" => "Kato Paphos",
                 "COUNTRY" => "Cyprus",
                 "CUST_NO" => 1351,
                 "FRST_CNTCT" => "19900412",
                 "NAME" => "Sight Diver",
                 "PHONE" => "357-6-876708",
                 "STATE_PROV" => "Dghdfghdfgh",
                 "STREET" => "1 Neptune Lane",
                 "ZIP_PST_CD" => ""
               }
               | _
             ] = ExDbase.parse(@sample_dbf)
    end

    test "should parse binary content" do
      assert [
               %{
                 "ASD" => nil,
                 "CITY" => "Kapaa Kauai",
                 "COUNTRY" => "U.s.a.",
                 "CUST_NO" => 1221,
                 "FRST_CNTCT" => "19900403",
                 "NAME" => "Kauai Dive Shoppe",
                 "PHONE" => "808-555-0269",
                 "STATE_PROV" => "Hi",
                 "STREET" => "4-976 Sugarloaf Hwy",
                 "ZIP_PST_CD" => "94766"
               },
               %{
                 "ASD" => nil,
                 "CITY" => "Freeport",
                 "COUNTRY" => "Bahamas",
                 "CUST_NO" => 1231,
                 "FRST_CNTCT" => "19810228",
                 "NAME" => "Unisco",
                 "PHONE" => "809-555-3915",
                 "STATE_PROV" => "",
                 "STREET" => "Po Box Z-547",
                 "ZIP_PST_CD" => ""
               },
               %{
                 "ASD" => nil,
                 "CITY" => "Kato Paphos",
                 "COUNTRY" => "Cyprus",
                 "CUST_NO" => 1351,
                 "FRST_CNTCT" => "19900412",
                 "NAME" => "Sight Diver",
                 "PHONE" => "357-6-876708",
                 "STATE_PROV" => "Dghdfghdfgh",
                 "STREET" => "1 Neptune Lane",
                 "ZIP_PST_CD" => ""
               }
               | _
             ] =
               @sample_dbf
               |> File.read!()
               |> ExDbase.parse_binary()
    end
  end
end
