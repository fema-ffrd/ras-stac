def prj_is_ras(prj_contents: str):
    """Verify if prj is from hec-ras model."""
    if "Proj Title" in prj_contents.split("\n")[0]:
        return True
    else:
        return False


def search_contents(lines: list, search_string: str, token: str = "=", expect_one: bool = True) -> list[str]:
    """Split a line by a token and returns the second half of the line if the search_string is found in the first half."""
    results = []
    for line in lines:
        if f"{search_string}{token}" in line:
            results.append(line.split(token)[1])

    if expect_one and len(results) > 1:
        raise ValueError(f"expected 1 result, got {len(results)}")
    elif expect_one and len(results) == 0:
        raise ValueError("expected 1 result, no results found")
    elif expect_one and len(results) == 1:
        return results[0]
    else:
        return results


def handle_spaces(line: str, lines: list[str]):
    """Handle spaces in the line."""
    if line in lines:
        return line
    elif handle_spaces_arround_equals(line.rstrip(" "), lines):
        return handle_spaces_arround_equals(line.rstrip(" "), lines)
    elif handle_spaces_arround_equals(line + " ", lines) in lines:
        return handle_spaces_arround_equals(line + " ", lines)
    else:
        raise ValueError(f"line: {line} not found in lines")


def handle_spaces_arround_equals(line: str, lines: list[str]) -> str:
    """Handle spaces in the line."""
    if line in lines:
        return line
    elif "= " in line:
        if line.replace("= ", "=") in lines:
            return line.replace("= ", "=")
    else:
        return line.replace("=", "= ")


def text_block_from_start_end_str(
    start_str: str, end_strs: list[str], lines: list, additional_lines: int = 0
) -> list[str]:
    """Search for an exact match to the start_str and return all lines from there to a line that contains the end_str."""
    start_str = handle_spaces(start_str, lines)

    start_index = lines.index(start_str)
    end_index = len(lines)
    for line in lines[start_index + 1 :]:
        if end_index != len(lines):
            break
        for end_str in end_strs:
            if end_str in line:
                end_index = lines.index(line) + additional_lines
                break
    return lines[start_index:end_index]


def text_block_from_start_str_to_empty_line(start_str: str, lines: list) -> list[str]:
    """Search for an exact match to the start_str and return all lines from there to the next empty line."""
    start_str = handle_spaces(start_str, lines)
    results = []
    in_block = False
    for line in lines:
        if line == start_str:
            in_block = True
            results.append(line)
            continue

        if in_block:
            if line == "":
                results.append(line)
                return results
            else:
                results.append(line)
    return results


def text_block_from_start_str_length(start_str: str, number_of_lines: int, lines: list) -> list[str]:
    """Search for an exact match to the start token and return a number of lines equal to number_of_lines."""
    start_str = handle_spaces(start_str, lines)
    results = []
    in_block = False
    for line in lines:
        if line == start_str:
            in_block = True
            continue
        if in_block:
            if len(results) >= number_of_lines:
                return results
            else:
                results.append(line)


def data_pairs_from_text_block(lines: list[str], width: int) -> list[tuple[float]]:
    """Split lines at given width to get paired data string. Split the string in half and convert to tuple of floats."""
    pairs = []
    for line in lines:
        for i in range(0, len(line), width):
            x = line[i : int(i + width / 2)]
            y = line[int(i + width / 2) : int(i + width)]
            pairs.append((float(x), float(y)))

    return pairs
