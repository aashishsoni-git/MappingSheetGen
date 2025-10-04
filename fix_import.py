import os

# Fix openai_mapper.py import
mapper_file = 'mapper/openai_mapper.py'
with open(mapper_file, 'r') as f:
    content = f.read()

content = content.replace(
    'from schemas import MappingPrediction',
    'from mapper.schemas import MappingPrediction'
)

with open(mapper_file, 'w') as f:
    f.write(content)

print("✅ Fixed openai_mapper.py import")
