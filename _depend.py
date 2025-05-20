import pkg_resources

pk = []
for p in pkg_resources.working_set:
    print(p.project_name)
    pk.append(f'"{p.project_name}"')
    
print(',\n'.join(pk))