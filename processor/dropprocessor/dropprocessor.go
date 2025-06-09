package dropprocessor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType("drop"),
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config { return &Config{} }

type Config struct {
	Drop_By_Prefix []string
}

type Processor struct {
	next consumer.Metrics
	cfg Config
}

func (cfg *Config) Validate() error { 
	if len(cfg.Drop_By_Prefix) == 0 {
		return fmt.Errorf("drop config is empty")
	}
	return nil
}

func (p *Processor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func createMetricsProcessor(_ context.Context, _ processor.Settings, cfg component.Config, next consumer.Metrics) (processor.Metrics, error) {
	pcfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsing error")
	}
	processor := Processor{next: next, cfg: *pcfg}
	return &processor, nil
}

func (p *Processor) Start(_ context.Context, _ component.Host) error { return nil }
func (p *Processor) Shutdown(_ context.Context) error { return nil }

func (p *Processor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if len(p.cfg.Drop_By_Prefix) == 0 {
		return p.next.ConsumeMetrics(ctx, md)
	}

	md.ResourceMetrics().RemoveIf(func(rmetrics pmetric.ResourceMetrics) bool {
		rmetrics.ScopeMetrics().RemoveIf(func(smetrics pmetric.ScopeMetrics) bool {
			smetrics.Metrics().RemoveIf(func(metric pmetric.Metric) bool {
				return false
			})
			return false
		})
		return false
	})
	return p.next.ConsumeMetrics(ctx, md)
}
