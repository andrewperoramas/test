import json

input_file = 'data.json'
output_file = 'data_array.json'

def convert_to_json_array(input_file, output_file):
    with open(input_file, 'r') as infile:
        lines = infile.readlines()

    # Remove any empty lines
    lines = [line.strip() for line in lines if line.strip()]

    # Convert each line into a JSON object and store in a list
    json_objects = [json.loads(line) for line in lines]

    # Write the JSON array to the output file
    with open(output_file, 'w') as outfile:
        json.dump(json_objects, outfile, indent=2)

    print(f"Converted data saved to {output_file}")

if __name__ == "__main__":
    convert_to_json_array(input_file, output_file)

