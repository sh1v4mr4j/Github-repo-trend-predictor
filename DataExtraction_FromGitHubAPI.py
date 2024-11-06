import requests
import pandas as pd

def fetch_github_repositories(query="", max_repos=5000):
    repositories = []
    page = 1
    per_page = 100  # Maximum number of results per page is 100

    # Check the rate limit before making requests
    rate_limit_response = requests.get('https://api.github.com/rate_limit', headers={'Authorization': 'EnterTokenHere'})
    rate_limit_data = rate_limit_response.json()
    remaining_requests = rate_limit_data['resources']['search']['remaining']
    reset_time = rate_limit_data['resources']['search']['reset']
    print(f"Remaining API requests: {remaining_requests}")
    print(f"Rate limit resets at: {reset_time}")  # Reset time in UTC timestamp format

    # Check if remaining requests are sufficient
    if remaining_requests <= 0:
        print("Rate limit exceeded. Please wait until the limit resets.")
        return []  # Or you can handle this scenario by sleeping until reset

    while len(repositories) < max_repos:
        # Modify the URL to sort by stars
        url = f'https://api.github.com/search/repositories?q={query}&sort=stars&order=desc&page={page}&per_page={per_page}'
        
        headers = {'Authorization': 'EnterTokenHere'}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            repositories.extend(items)

            print(f"Page {page} fetched with {len(items)} repositories.")  # Debugging line

            if len(items) < per_page:
                break  # No more items available

            page += 1  # Move to the next page
        else:
            print(f"Error: {response.status_code} - {response.json().get('message', '')}")
            break

    print(f"Total repositories fetched: {len(repositories)}")  # Debugging line
    return repositories[:max_repos]

# Example usage without query filters, but sorted by stars
query = "stars:>0"  # Filter repositories with more than 100 stars
repositories = fetch_github_repositories(query)  # No filters applied, but sorted by stars

# Output the repositories
repo_data = []
for repo in repositories:
    repo_data.append({
        'Id': repo['id'],
        'Name': repo['name'],
        'FullName': repo['full_name'],
        'HtmlUrl': repo['html_url'],
        'Description': repo['description'],
        'Language': repo['language'],
        'CreatedAt': repo['created_at'],
        'UpdatedAt': repo['updated_at'],
        'PushedAt': repo['pushed_at'],
        'OpenIssuesCount': repo['open_issues_count'],
        'ForksCount': repo['forks_count'],
        'StargazersCount': repo['stargazers_count'],
        'WatchersCount': repo['watchers_count'],
        'Size': repo['size'],
        'OwnerLogin': repo['owner']['login'],
        'OwnerType': repo['owner']['type'],
        'License': repo['license']['name'] if repo['license'] else 'NA'
    })

# Convert to DataFrame
df = pd.DataFrame(repo_data)
filename = "most_popular_repositories_5000.csv"
# Save DataFrame to CSV
df.to_csv(filename, index=False, encoding='utf-8')
print(f"Data saved to {filename}")
