#!/usr/bin/env python3
"""Sync the roadmap in `.github/roadmap.yml` to GitHub labels, milestones, and issues.

The file is the source of truth and the sync is idempotent. Milestones are matched by
title and issues by the `id` the renderer writes into each body as an HTML comment, so
a rerun updates what it created before instead of creating it again.

Modes:

    --plan-only   parse and validate the file, print what it declares, touch no network
    --dry-run     read GitHub, print the create and update actions, write nothing
    (default)     apply

Every mode first checks that ROADMAP.md still names every milestone and issue this
file declares, so the prose roadmap cannot drift from the issues it describes.

The default mode needs `GITHUB_TOKEN` with `issues: write`.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import pathlib
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

import yaml

DEFAULT_ROADMAP = ".github/roadmap.yml"
DEFAULT_DOCUMENT = "ROADMAP.md"
DEFAULT_REPOSITORY = "krabka-io/krabka-streams-java"
MARKER = "<!-- roadmap-id: {id} -->"
MARKER_PATTERN = re.compile(r"<!--\s*roadmap-id:\s*([A-Za-z0-9._-]+)\s*-->")
ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")
COLOR_PATTERN = re.compile(r"^[0-9a-fA-F]{6}$")
USER_AGENT = "krabka-streams-java-roadmap-sync"


class RoadmapError(Exception):
    """A problem with the roadmap file itself."""


# --------------------------------------------------------------------------- model


def load_roadmap(path: str) -> dict[str, Any]:
    """Reads the roadmap file and validates every reference it makes."""
    with open(path, encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    if not isinstance(document, dict):
        raise RoadmapError(f"{path}: expected a mapping at the top level")

    unknown = set(document) - {"labels", "milestones", "issues"}
    if unknown:
        raise RoadmapError(f"{path}: unknown top-level keys {sorted(unknown)}")

    labels = document.get("labels") or []
    milestones = document.get("milestones") or []
    issues = document.get("issues") or []

    for label in labels:
        require(label, "name", str, "label")
        color = label.get("color", "")
        if not COLOR_PATTERN.match(str(color)):
            raise RoadmapError(f"label `{label['name']}`: color must be six hex digits")

    for milestone in milestones:
        require(milestone, "title", str, "milestone")
        milestone["due_on"] = normalize_due_on(milestone.get("due_on"))

    declared_labels = {label["name"] for label in labels}
    declared_milestones = {milestone["title"] for milestone in milestones}
    seen: set[str] = set()
    for issue in issues:
        require(issue, "id", str, "issue")
        require(issue, "title", str, f"issue `{issue.get('id')}`")
        require(issue, "body", str, f"issue `{issue.get('id')}`")
        identifier = issue["id"]
        if not ID_PATTERN.match(identifier):
            raise RoadmapError(f"issue `{identifier}`: id must be lowercase kebab-case")
        if identifier in seen:
            raise RoadmapError(f"issue `{identifier}`: duplicate id")
        seen.add(identifier)
        milestone = issue.get("milestone")
        if milestone is not None and milestone not in declared_milestones:
            raise RoadmapError(f"issue `{identifier}`: unknown milestone `{milestone}`")
        for name in issue.get("labels") or []:
            if name not in declared_labels:
                raise RoadmapError(f"issue `{identifier}`: undeclared label `{name}`")

    return {"labels": labels, "milestones": milestones, "issues": issues}


def require(mapping: dict[str, Any], key: str, kind: type, what: str) -> None:
    value = mapping.get(key)
    if not isinstance(value, kind) or (kind is str and not value.strip()):
        raise RoadmapError(f"{what}: `{key}` is required and must be a non-empty {kind.__name__}")


def normalize_due_on(value: Any) -> str | None:
    """Accepts a date or `YYYY-MM-DD` and returns the timestamp the API wants."""
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        value = value.date()
    if isinstance(value, datetime.date):
        return f"{value.isoformat()}T00:00:00Z"
    text = str(value)
    try:
        datetime.date.fromisoformat(text[:10])
    except ValueError as error:
        raise RoadmapError(f"milestone due_on `{text}` is not a date") from error
    return f"{text[:10]}T00:00:00Z"


def check_document(roadmap: dict[str, Any], path: str) -> None:
    """Fails when ROADMAP.md has drifted from the file that generates the issues."""
    try:
        prose = pathlib.Path(path).read_text(encoding="utf-8")
    except OSError as error:
        raise RoadmapError(f"{path}: {error}") from error

    missing = [m["title"] for m in roadmap["milestones"] if m["title"] not in prose]
    missing += [i["title"] for i in roadmap["issues"] if i["title"] not in prose]
    if missing:
        raise RoadmapError(
            f"{path} does not mention: " + "; ".join(missing) + ". "
            "The prose roadmap and the issues it describes have drifted."
        )


def render_body(issue: dict[str, Any], repository: str, server: str) -> str:
    """Renders the issue body, with the footer and the identity marker appended."""
    roadmap = f"{server}/{repository}/blob/main/ROADMAP.md"
    return (
        f"{issue['body'].rstrip()}\n\n"
        "---\n\n"
        f"Tracked on the [roadmap]({roadmap}). Change this issue by editing "
        "`.github/roadmap.yml`: the next sync overwrites the title and body written here.\n\n"
        f"{MARKER.format(id=issue['id'])}\n"
    )


def normalize(text: str | None) -> str:
    return (text or "").replace("\r\n", "\n").strip()


# ----------------------------------------------------------------------------- api


class GitHub:
    """The slice of the REST API this sync needs."""

    def __init__(self, repository: str, token: str, api_url: str) -> None:
        self.repository = repository
        self.token = token
        self.api_url = api_url.rstrip("/")

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        url = f"{self.api_url}{path}" if path.startswith("/") else path
        payload = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(url, data=payload, method=method)
        request.add_header("Accept", "application/vnd.github+json")
        request.add_header("Authorization", f"Bearer {self.token}")
        request.add_header("X-GitHub-Api-Version", "2022-11-28")
        request.add_header("User-Agent", USER_AGENT)
        if payload is not None:
            request.add_header("Content-Type", "application/json")

        for attempt in range(4):
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    return json.loads(response.read().decode("utf-8") or "null")
            except urllib.error.HTTPError as error:
                detail = error.read().decode("utf-8", "replace")
                retriable = error.code >= 500 or error.code in (403, 429)
                if not retriable or attempt == 3:
                    raise SystemExit(
                        f"{method} {url} failed with {error.code}: {detail}"
                    ) from error
                delay = float(error.headers.get("Retry-After") or 2**attempt)
                print(f"  {error.code} from {method} {url}; retrying in {delay:.0f}s")
                time.sleep(delay)
            except urllib.error.URLError as error:
                if attempt == 3:
                    raise SystemExit(f"{method} {url} failed: {error.reason}") from error
                time.sleep(2**attempt)
        raise SystemExit(f"{method} {url}: exhausted retries")

    def paginate(self, path: str, **query: str) -> list[Any]:
        items: list[Any] = []
        page = 1
        while True:
            parameters = {**query, "per_page": "100", "page": str(page)}
            batch = self.request("GET", f"{path}?{urllib.parse.urlencode(parameters)}")
            if not batch:
                return items
            items.extend(batch)
            if len(batch) < 100:
                return items
            page += 1

    def labels(self) -> list[dict[str, Any]]:
        return self.paginate(f"/repos/{self.repository}/labels")

    def milestones(self) -> list[dict[str, Any]]:
        return self.paginate(f"/repos/{self.repository}/milestones", state="all")

    def issues(self) -> list[dict[str, Any]]:
        raw = self.paginate(f"/repos/{self.repository}/issues", state="all")
        return [item for item in raw if "pull_request" not in item]


# ---------------------------------------------------------------------------- sync


class Sync:
    """Applies the roadmap, recording one line per action taken or skipped."""

    def __init__(self, api: GitHub, roadmap: dict[str, Any], server: str, dry_run: bool) -> None:
        self.api = api
        self.roadmap = roadmap
        self.server = server
        self.dry_run = dry_run
        self.actions: list[str] = []

    def record(self, line: str) -> None:
        self.actions.append(line)
        print(("would " if self.dry_run else "") + line)

    def run(self) -> list[str]:
        self.sync_labels()
        milestones = self.sync_milestones()
        self.sync_issues(milestones)
        return self.actions

    def sync_labels(self) -> None:
        existing = {label["name"]: label for label in self.api.labels()}
        for label in self.roadmap["labels"]:
            name, color = label["name"], label["color"].lower()
            description = label.get("description", "")
            current = existing.get(name)
            if current is None:
                self.record(f"create label `{name}`")
                if not self.dry_run:
                    self.api.request(
                        "POST",
                        f"/repos/{self.api.repository}/labels",
                        {"name": name, "color": color, "description": description},
                    )
            elif current.get("color", "").lower() != color or (
                current.get("description") or ""
            ) != description:
                self.record(f"update label `{name}`")
                if not self.dry_run:
                    self.api.request(
                        "PATCH",
                        f"/repos/{self.api.repository}/labels/{urllib.parse.quote(name)}",
                        {"new_name": name, "color": color, "description": description},
                    )

    def sync_milestones(self) -> dict[str, int]:
        existing = {milestone["title"]: milestone for milestone in self.api.milestones()}
        numbers: dict[str, int] = {}
        for milestone in self.roadmap["milestones"]:
            title = milestone["title"]
            desired = {"title": title, "description": milestone.get("description", "").strip()}
            if milestone["due_on"]:
                desired["due_on"] = milestone["due_on"]
            current = existing.get(title)
            if current is None:
                self.record(f"create milestone `{title}`")
                if self.dry_run:
                    continue
                created = self.api.request(
                    "POST", f"/repos/{self.api.repository}/milestones", desired
                )
                numbers[title] = created["number"]
                continue

            numbers[title] = current["number"]
            changed = normalize(current.get("description")) != normalize(desired["description"])
            due = desired.get("due_on")
            if due and (current.get("due_on") or "")[:10] != due[:10]:
                changed = True
            if changed:
                self.record(f"update milestone `{title}`")
                if not self.dry_run:
                    self.api.request(
                        "PATCH",
                        f"/repos/{self.api.repository}/milestones/{current['number']}",
                        desired,
                    )
        return numbers

    def sync_issues(self, milestones: dict[str, int]) -> None:
        by_id: dict[str, dict[str, Any]] = {}
        for issue in self.api.issues():
            match = MARKER_PATTERN.search(issue.get("body") or "")
            if match:
                by_id[match.group(1)] = issue

        for issue in self.roadmap["issues"]:
            identifier = issue["id"]
            title = issue["title"]
            body = render_body(issue, self.api.repository, self.server)
            labels = list(issue.get("labels") or [])
            milestone = milestones.get(issue.get("milestone") or "")
            current = by_id.get(identifier)

            if current is None:
                self.record(f"create issue `{identifier}`: {title}")
                if self.dry_run:
                    continue
                payload: dict[str, Any] = {"title": title, "body": body, "labels": labels}
                if milestone is not None:
                    payload["milestone"] = milestone
                created = self.api.request(
                    "POST", f"/repos/{self.api.repository}/issues", payload
                )
                print(f"  {created['html_url']}")
                continue

            number = current["number"]
            if current.get("state") == "closed":
                self.record(f"skip closed issue #{number} `{identifier}`")
                continue

            update: dict[str, Any] = {}
            if current.get("title") != title:
                update["title"] = title
            if normalize(current.get("body")) != normalize(body):
                update["body"] = body
            present = {label["name"] for label in current.get("labels") or []}
            if not set(labels) <= present:
                update["labels"] = sorted(present | set(labels))
            current_milestone = (current.get("milestone") or {}).get("number")
            if milestone is not None and current_milestone != milestone:
                update["milestone"] = milestone
            if not update:
                continue

            self.record(f"update issue #{number} `{identifier}`: {', '.join(sorted(update))}")
            if not self.dry_run:
                self.api.request(
                    "PATCH", f"/repos/{self.api.repository}/issues/{number}", update
                )


# ---------------------------------------------------------------------------- main


def print_plan(roadmap: dict[str, Any]) -> None:
    print(f"{len(roadmap['labels'])} labels")
    for label in roadmap["labels"]:
        print(f"  {label['name']}")
    print(f"{len(roadmap['milestones'])} milestones")
    for milestone in roadmap["milestones"]:
        due = milestone["due_on"][:10] if milestone["due_on"] else "no due date"
        print(f"  {milestone['title']} ({due})")
    print(f"{len(roadmap['issues'])} issues")
    for issue in roadmap["issues"]:
        print(f"  [{issue['id']}] {issue['title']} -> {issue.get('milestone', 'no milestone')}")


def write_summary(actions: list[str], dry_run: bool) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    heading = "Roadmap sync (dry run)" if dry_run else "Roadmap sync"
    lines = [f"## {heading}", ""]
    lines += [f"- {action}" for action in actions] or ["Nothing to do; GitHub already matches."]
    with open(path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--roadmap", default=DEFAULT_ROADMAP, help="path to the roadmap file")
    parser.add_argument("--doc", default=DEFAULT_DOCUMENT, help="prose roadmap to check for drift")
    parser.add_argument(
        "--repo",
        default=os.environ.get("GITHUB_REPOSITORY", DEFAULT_REPOSITORY),
        help="owner/repo to sync",
    )
    parser.add_argument("--dry-run", action="store_true", help="read GitHub, write nothing")
    parser.add_argument(
        "--plan-only", action="store_true", help="validate the file without calling GitHub"
    )
    arguments = parser.parse_args(argv)

    try:
        roadmap = load_roadmap(arguments.roadmap)
        check_document(roadmap, arguments.doc)
    except (RoadmapError, yaml.YAMLError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    if arguments.plan_only:
        print_plan(roadmap)
        return 0

    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        print("error: GITHUB_TOKEN is not set", file=sys.stderr)
        return 1

    api = GitHub(
        arguments.repo, token, os.environ.get("GITHUB_API_URL", "https://api.github.com")
    )
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com").rstrip("/")
    actions = Sync(api, roadmap, server, arguments.dry_run).run()
    if not actions:
        print("nothing to do; GitHub already matches the roadmap")
    write_summary(actions, arguments.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
