import re

# assume we get things exactly formatted
company_re = re.compile(r'<h1 class="cmp-section-first-header">(.*?)</h1>')

entire_posting_re = re.compile(r'<li class="cmp-section cmp-job-entry">(.*?)</li>.+?')
jobtitle_re = re.compile(r'<h3><a class="cmp-job-url" href=".+?" target=".+?" rel=".+?">(.*?)</a></h3>')
location_re = re.compile(r'<div class="cmp-note">(.*?)</div>')
description_re = re.compile(r'<div class="cmp-job-snippet">(.*?)</div>')
time_re = re.compile(r'<div class="cmp-note cmp-relative-time">(.*?)</div>')

def find_postings_in_page(fulltext):
	company = company_re.search(fulltext)
	list_to_return = []
	company_name = None

	if company:
		company_name = company.group(1)

		for this_li in entire_posting_re.finditer(fulltext):
			try:
				entire_posting = this_li.group(1)
				jobtitle = jobtitle_re.search(entire_posting)
				description = description_re.search(entire_posting)
				location = location_re.search(entire_posting)
				time = time_re.search(entire_posting)

				list_to_return.append({
					"jobtitle": jobtitle.group(1),
					"description": description.group(1),
					"location": location.group(1),
					"time": time.group(1)
					})
				print(jobtitle.group(1))
			except AttributeError:
				print("Error while matching regex")
	return (company_name, list_to_return)

with open('4.html', 'rb') as f:
	fulltext = f.read().decode('utf-8')

	print(find_postings_in_page(fulltext))
	# company = company_re.search(fulltext)
	# if company:
	# 	print(company.group(1))
	# 	print('\n\n')

	# 	for this_li in entire_posting_re.finditer(fulltext):
	# 		entire_posting = this_li.group(1)
	# 		heading = heading_re.search(entire_posting)
	# 		print('--- HEADING ---')
	# 		if heading:
	# 			print(heading.group(1))

	# 		description = description_re.search(entire_posting)
	# 		print('--- DESCRIPTION ---')
	# 		if description:
	# 			print(description.group(1))

	# 		location = location_re.search(entire_posting)
	# 		print('--- LOCATION ---')
	# 		if location:
	# 			print(location.group(1))

	# 		time = time_re.search(entire_posting)
	# 		print('--- TIME ---')
	# 		if time:
	# 			print(time.group(1))

	# 		print('\n')
