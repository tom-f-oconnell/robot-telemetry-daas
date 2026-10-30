<?xml version="1.0"?>
<!-- The provider attaches a `disk { file = ... }` as raw; the root disks are qcow2. -->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output omit-xml-declaration="yes" indent="yes"/>
  <xsl:template match="node()|@*">
    <xsl:copy><xsl:apply-templates select="node()|@*"/></xsl:copy>
  </xsl:template>
  <xsl:template match="/domain/devices/disk[@device='disk']/driver/@type">
    <xsl:attribute name="type">qcow2</xsl:attribute>
  </xsl:template>
</xsl:stylesheet>
