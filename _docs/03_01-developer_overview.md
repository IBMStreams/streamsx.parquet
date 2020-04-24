---
title: "Getting Started"
permalink: /docs/developer/overview/
excerpt: "Contributing to this toolkits development."
last_modified_at: 2020-04-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}


## Build

To build the toolkit, run the following ant command:

    ant all

## Clean

To clean the toolkit, run the following ant command:

    ant clean

## Create a release

To create a release of the toolkit, run the following ant command:

    ant release

## Build the samples

To build all sample applications, run the following commands:

    cd samples
    make all
