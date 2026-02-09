
import re

tex_file_path = '/Applications/XAMPP/xamppfiles/htdocs/aniskoubaa.org/se446/big_data_course/lectures/week03/slides/SE446_W03B_mapreduce_python.tex'

with open(tex_file_path, 'r') as f:
    text = f.read()

# Check braces
brace_stack = []
for i, char in enumerate(text):
    if char == '{':
        brace_stack.append(i)
    elif char == '}':
        if brace_stack:
            brace_stack.pop()
        else:
            print(f"Extra closing brace at position {i}")

if brace_stack:
    line_num = text[:brace_stack[-1]].count('\n') + 1
    print(f"Unclosed opening brace at position {brace_stack[-1]} (Line {line_num})")
else:
    print("Braces balanced.")

# Check environments
env_stack = []
lines = text.split('\n')
for i, line in enumerate(lines):
    line_num = i + 1
    # Simple regex for begin/end (ignoring comments for now, assuming no nested braces in env name)
    # This is a heuristic check.
    
    # Remove comments
    display_line = line.split('%')[0]
    
    begins = re.finditer(r'\\begin\{([^}]+)\}', display_line)
    ends = re.finditer(r'\\end\{([^}]+)\}', display_line)
    
    # Collect all tokens in order
    tokens = []
    for m in begins:
        tokens.append(('begin', m.group(1), m.start()))
    for m in ends:
        tokens.append(('end', m.group(1), m.start()))
    
    tokens.sort(key=lambda x: x[2])
    
    for type, name, _ in tokens:
        if type == 'begin':
            env_stack.append((name, line_num))
        elif type == 'end':
            if not env_stack:
                print(f"Error at line {line_num}: Unexpected \\end{{{name}}}")
            else:
                last_name, last_line = env_stack.pop()
                if last_name != name:
                    print(f"Error at line {line_num}: Mismatched environment. Expected \\end{{{last_name}}} (started at {last_line}), found \\end{{{name}}}")

if env_stack:
    for name, line_num in env_stack:
        print(f"Error: Unclosed \\begin{{{name}}} at line {line_num}")
else:
    print("Environments balanced.")
