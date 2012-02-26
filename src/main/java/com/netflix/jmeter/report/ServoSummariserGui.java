package com.netflix.jmeter.report;

import java.awt.BorderLayout;

import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.visualizers.gui.AbstractListenerGui;

public class ServoSummariserGui extends AbstractListenerGui
{
    private static final long serialVersionUID = 1L;
    private static final String LABEL = "Epic Summariser";

    public ServoSummariserGui()
    {
        super();
        init();
    }

    public String getLabelResource()
    {
        return LABEL;
    }
    
    @Override
    public String getStaticLabel() 
    {
        return LABEL;
    }

    @Override
    public void configure(TestElement el)
    {
        super.configure(el);
    }

    public TestElement createTestElement()
    {
        AbstractSummariser summariser = new ServoSummariser();
        modifyTestElement(summariser);
        return summariser;
    }

    public void modifyTestElement(TestElement summariser)
    {
        super.configureTestElement(summariser);
    }

    private void init()
    {
        setLayout(new BorderLayout());
        setBorder(makeBorder());
        add(makeTitlePanel(), BorderLayout.NORTH);
    }
}
