import json


def duplicate_rows_and_save(input_file_path, output_file_path):
    def duplicate_rows(data):
        new_data = []
        for row in data:
            rdates = row.get("daily_data", {}).get("date", [])
            river_discharges = row.get("daily_data", {}).get("river_discharge", [])
            if isinstance(rdates, list) and isinstance(river_discharges, list):
                for rdate, river_discharge in zip(rdates, river_discharges):
                    new_row = {
                        "latitude": row["latitude"],
                        "longitude": row["longitude"],
                        "rdate": rdate,
                        "river_discharge": river_discharge,
                        "elevation": row["elevation"],
                        "timezone": row["timezone"],
                        "timezone_abbreviation": row["timezone_abbreviation"],
                    }

                    new_data.append(new_row)

            else:
                new_data.append(row)

        return new_data

    with open(input_file_path, "r") as file:

        json_data = json.load(file)

    new_json_data = duplicate_rows(json_data)

    with open(output_file_path, "w") as file:

        json.dump(new_json_data, file, indent=2)

input_file_path = "../data_extraction/output_data.json"

output_file_path = "flood_output_file.json"

duplicate_rows_and_save(input_file_path, output_file_path)