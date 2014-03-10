PROJECT SLIDER
===

Introduction
---

**SLIDER: A collection of tools and technologies to simplify the packaging, deployment and management of long-running applications on YARN.**

- Availability (always-on) - YARN works with the application to ensure recovery or restart of running application components.
- Flexibility (dynamic scaling) - YARN provides the application with the facilities to allow for scale-up or scale-down
- Resource Mgmt (optimization) - YARN handles allocation of cluster resources.

Terminology
---

- **Apps on YARN**
 - Application written to run directly on YARN
 - Packaging, deployment and lifecycle management are custom built for each application

- **Slider Apps**
 - Applications deployed and managed on YARN using Slider
 - Use of slider minimizes custom code for deployment + lifecycle management
- Requires apps to follow Slider guidelines and packaging ("Sliderize")

Specifications
---

Information / material to co-exist in the project. The entry point for partners that will leverage Slider.

- [Specifications for AppPackage](application_package.md)
- [Documentation for the SliderCLI](apps_on_yarn_cli.md)
- [Specifications for Application Definition](application_definition.md)
- [Specifications for Configuration](application_configuration.md)
- [Specification of Resources](resource_specification.md)
- [Specifications InstanceConfiguration](application_instance_configuration.md)
- [Guidelines for Clients and Client Applications](canonical_scenarios.md)
- [Documentation for "General Developer Guidelines"](app_developer_guideline.md)
